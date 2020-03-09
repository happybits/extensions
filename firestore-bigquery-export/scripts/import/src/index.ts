#!/usr/bin/env node

/*
 * Copyright 2019 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import * as firebase from "firebase-admin";
import * as fs from "fs";
import * as inquirer from "inquirer";
import * as util from "util";

import {
  ChangeType,
  FirestoreBigQueryEventHistoryTracker,
  FirestoreDocumentChangeEvent,
} from "./../../../firestore-bigquery-change-tracker/src/index";

// For reading cursor position.
const exists = util.promisify(fs.exists);
const write = util.promisify(fs.writeFile);
const read = util.promisify(fs.readFile);
const unlink = util.promisify(fs.unlink);

const BIGQUERY_VALID_CHARACTERS = /^[a-zA-Z0-9_]+$/;
const FIRESTORE_VALID_CHARACTERS = /^[^\/]+$/;
const FIRESTORE_PATH_VALID_CHARACTERS = /^[a-zA-Z{}\/ _]+$/;

const WILDCARD = /^{.*}/;
const HAS_WILDCARD = /^.*{.*}.*/;
const FIRESTORE_COLLECTION_NAME_MAX_CHARS = 6144;
const BIGQUERY_RESOURCE_NAME_MAX_CHARS = 1024;

const FIRESTORE_DEFAULT_DATABASE = "(default)";

const isWildCardPath = (path) => path.match(HAS_WILDCARD);

// Ensure the path provided has the correct format
const hasCorrectPathFormat = (steps) => {
  return steps.reduce((acc, step) => {
    if (!acc.correct) {
      return acc;
    }
    if ((isWildCardPath(step) && isWildCardPath(acc.prev)) || (!isWildCardPath(step) && !isWildCardPath(acc.prev))) {
      acc.correct = false;
      return acc;
    }
    acc.prev = step;
    return acc;
  }, {correct: true, prev: "{}"}).correct;
};

const validateInput = (
  value: string,
  name: string,
  regex: RegExp,
  sizeLimit: number
) => {
  if (!value || value === "" || value.trim() === "") {
    return `Please supply a ${name}`;
  }
  if (value.length >= sizeLimit) {
    return `${name} must be at most ${sizeLimit} characters long`;
  }
  if (!value.match(regex)) {
    return `The ${name} must only contain letters, spaces, or underscores`;
  }

  if (name === "collection path") {
    let steps = value.split("/");
    if (steps[steps.length - 1].match(WILDCARD)) {
      return `${name} must not end in a wildcard: ${value}`;
    }
    
    if(!hasCorrectPathFormat(steps)) {
      return `${name} must follow format 'aaa/{wildcard}/bbb/.../zzz`
    }
    
  }
  return true;
};

const questions = [
  {
    message: "What is your Firebase project ID?",
    name: "projectId",
    type: "input",
    validate: (value) =>
      validateInput(
        value,
        "project ID",
        FIRESTORE_VALID_CHARACTERS,
        FIRESTORE_COLLECTION_NAME_MAX_CHARS
      ),
  },
  {
    message:
      "What is the path of the the Cloud Firestore Collection you would like to import from? " +
      "(This may, or may not, be the same Collection for which you plan to mirror changes.)",
    name: "sourceCollectionPath",
    type: "input",
    validate: (value) =>
      validateInput(
        value,
        "collection path",
        FIRESTORE_PATH_VALID_CHARACTERS,
        FIRESTORE_COLLECTION_NAME_MAX_CHARS
      ),
  },
  {
    message:
      "What is the ID of the BigQuery dataset that you would like to use? (A dataset will be created if it doesn't already exist)",
    name: "datasetId",
    type: "input",
    validate: (value) =>
      validateInput(
        value,
        "dataset",
        BIGQUERY_VALID_CHARACTERS,
        BIGQUERY_RESOURCE_NAME_MAX_CHARS
      ),
  },
  {
    message:
      "What is the identifying prefix of the BigQuery table that you would like to import to? (A table will be created if one doesn't already exist)",
    name: "tableId",
    type: "input",
    validate: (value) =>
      validateInput(
        value,
        "table",
        BIGQUERY_VALID_CHARACTERS,
        BIGQUERY_RESOURCE_NAME_MAX_CHARS
      ),
  },
  {
    message:
      "How many documents should the import stream into BigQuery at once?",
    name: "batchSize",
    type: "input",
    default: 300,
    validate: (value) => {
      return parseInt(value, 10) > 0;
    },
  },
];

const run = async (): Promise<number> => {
  const {
    projectId,
    sourceCollectionPath,
    datasetId,
    tableId,
    batchSize,
  } = await inquirer.prompt(questions);

  const batch = parseInt(batchSize);
  const rawChangeLogName = `${tableId}_raw_changelog`;

  // Initialize Firebase
  firebase.initializeApp({
    credential: firebase.credential.applicationDefault(),
    databaseURL: `https://${projectId}.firebaseio.com`,
  });
  // Set project ID so it can be used in BigQuery intialization
  process.env.PROJECT_ID = projectId;
  process.env.GOOGLE_CLOUD_PROJECT = projectId;

  // We pass in the application-level "tableId" here. The tracker determines
  // the name of the raw changelog from this field.
  const dataSink = new FirestoreBigQueryEventHistoryTracker({
    tableId: tableId,
    datasetId: datasetId,
  });

  console.log(
    `Importing data from Cloud Firestore Collection: ${sourceCollectionPath}, to BigQuery Dataset: ${datasetId}, Table: ${rawChangeLogName}`
  );

  let totalDocsRead = 0;
  let totalRowsImported = 0;

  // If the path contains sub-collections / wildcard values, we must traverse
  // the data tree and collect the paths located at the leaves.
  let collectionPaths;
  if (isWildCardPath) {
    let steps = sourceCollectionPath.split("/");
    let collections;
    // Track collections to be imported
    let i = 0;
    for (let step of steps) {
      // First iteration requires top level collection
      if (!collections) {
        collections = [firebase.firestore()];
      }

      // If Wildcard pattern {...}
      if (step.match(WILDCARD)) {
        // All refs should be CollectionReferences
        // Get all docs from collection reference
        collections = await collections.reduce(async (acc, col) => {
          let docs = await col.listDocuments();
          acc = await Promise.resolve(acc);
          if (docs.length === 0) {
            return acc;
          }
          return acc.concat(docs);
        }, []);
      } else {
        // If collection step, all refs should be DocumentReferences
        // If step is the last, return the path only
        if (i === steps.length - 1) {
          collections = collections.map((ref) => ref.collection(step).path);
        } else {
          collections = collections.map((ref) => ref.collection(step));
        }
      }
      i += 1;
    }
    collectionPaths = collections;
  } else {
    collectionPaths = [sourceCollectionPath];
  }

  const slugifyPath = (path) => {
    return path.split("/").join("_");
  };

  // Sort cursor position files across entire job. Only delete them once
  // script finishes. This ensures checkpointing will maintain the position
  // across files.
  let cursorPositionFiles = new Set<string>();
  let rows: FirestoreDocumentChangeEvent[] = [];
  let dataSinkPromises = [];
  // In order to batch across multiple collections, must globally track
  let i = 0;
  for (let path of collectionPaths) {
    // Build the data row with a 0 timestamp. This ensures that all other
    // operations supersede imports when listing the live documents.
    let cursor;

    let cursorPositionFile =
      __dirname +
      `/from-${slugifyPath(
        path
      )}-to-${projectId}_${datasetId}_${rawChangeLogName}`;

    if (await exists(cursorPositionFile)) {
      let cursorDocumentId = (await read(cursorPositionFile)).toString();
      cursor = await firebase
        .firestore()
        .collection(sourceCollectionPath)
        .doc(cursorDocumentId)
        .get();
      console.log(
        `Resuming import of Cloud Firestore Collection ${sourceCollectionPath} from document ${cursorDocumentId}.`
      );
    }

    do {
      if (cursor) {
        await write(cursorPositionFile, cursor.id);
        cursorPositionFiles.add(cursorPositionFile);
      }

      // Batch size for sub-query: batch - i
      let query = firebase
        .firestore()
        .collection(path)
        .limit(batch - i);
      if (cursor) {
        query = query.startAfter(cursor);
      }
      const snapshot = await query.get();
      const docs = snapshot.docs;
      if (docs.length === 0) {
        break;
      }
      totalDocsRead += docs.length;
      cursor = docs[docs.length - 1];
      const rowsBatch: FirestoreDocumentChangeEvent[] = docs.map((snapshot) => {
        return {
          timestamp: new Date(0).toISOString(), // epoch
          operation: ChangeType.IMPORT,
          documentName: `projects/${projectId}/databases/${FIRESTORE_DEFAULT_DATABASE}/documents/${
            snapshot.ref.path
          }`,
          eventId: "",
          data: snapshot.data(),
        };
      });
      totalRowsImported += rowsBatch.length;
      i += rowsBatch.length;
      rows = rows.concat(rowsBatch);

      // Only record rows if N total tracked rows === batch
      // On first dataSink record, need to check to see if dataset
      // exists. If it doesn't, BigQuery will throw a bunch of errors if
      // we record asynchronously. `await` first record to ensure the resources
      // are created.
      if (i >= batch) {
        if (dataSinkPromises.length === 0) {
          let resourcesExist = await dataSink.bq.dataset(datasetId).exists();
          // Create resources via record
          if (!resourcesExist[0]) {
            await dataSink.record(rows);
            rows = [];
            i = 0;
            continue;
          }
        }
        dataSinkPromises.push(dataSink.record(rows));
        rows = [];
        i = 0;
      }
    } while (true);
  }

  // Ensure all tracked rows are recorded
  if (rows.length !== 0) {
    dataSinkPromises.push(dataSink.record(rows));
  }

  await Promise.all(dataSinkPromises);

  for (let cursorPositionFile of cursorPositionFiles) {
    try {
      await unlink(cursorPositionFile);
    } catch (e) {
      console.log(
        `Error unlinking journal file ${cursorPositionFile} after successful import: ${e.toString()}`
      );
    }
  }

  return totalRowsImported;
};

run()
  .then((rowCount) => {
    console.log("---------------------------------------------------------");
    console.log(`Finished importing ${rowCount} Firestore rows to BigQuery`);
    console.log("---------------------------------------------------------");
    process.exit();
  })
  .catch((error) => {
    console.error(
      `Error importing Collection to BigQuery: ${error.toString()}`
    );
    process.exit(1);
  });
