# SheetORM for Google Apps Script

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

An ORM-like library for Google Sheets designed to run in the Google Apps Script environment. `SheetORM` simplifies data manipulation by providing a clear and expressive API for CRUD (Create, Read, Update, Delete) operations, allowing you to treat a Google Sheet like a simple database.

This library uses the fast Google Visualization API for read operations and standard `SpreadsheetApp` services for reliable write operations.

## Features

-   **Simple & Expressive API**: Methods like `create`, `find`, `updateById`, `deleteMany` make code more readable.
-   **Fast Read Operations**: Utilizes the Google Visualization API for efficient data retrieval (`find*`, `query`).
-   **Reliable Write Operations**: Uses standard `SpreadsheetApp` services with `LockService` to prevent race conditions.
-   **Flexible Querying**:
    -   Build structured queries with `findMany()`.
    -   Execute powerful "raw" queries using `query()` with a user-friendly syntax (e.g., `select [Column Name] where ...`).
-   **No External Dependencies**: Runs entirely within the Google Apps Script environment.

## Setup

1.  Open your Google Apps Script project.
2.  Create a new script file (e.g., `SheetORM.gs`).
3.  Copy the entire code from the `SheetORM.gs` file in this repository and paste it into the new script file in your project.
4.  You can now use the `SheetORM` function in your other script files.

## Quick Start

Imagine you have a sheet named "Tasks" with columns: "id", "TaskName", "Status", and "Priority".

```javascript
function myFunction() {
  // 1. Initialize the ORM for your sheet
  // Make sure to configure the 'idField' if it's not named 'id'.
  const taskSheet = SheetORM("Tasks", { idField: "id" });
  
  if (!taskSheet) {
    Logger.log("Failed to initialize SheetORM. Check sheet name and permissions.");
    return;
  }

  // 2. Create a new record
  const newId = "task-" + new Date().getTime(); // Generate a unique ID
  const wasCreated = taskSheet.create({
    id: newId,
    TaskName: "Finalize the library documentation",
    Status: "In Progress",
    Priority: "High"
  });

  if (wasCreated) {
    Logger.log("New task created successfully!");
  }

  // 3. Find a record by its ID
  const foundTask = taskSheet.findById(newId);
  if (foundTask) {
    Logger.log("Found task: " + foundTask.TaskName); // Logs: "Found task: Finalize the library documentation"
  }

  // 4. Update a record by its ID
  const wasUpdated = taskSheet.updateById(newId, {
    Status: "Completed"
  });
  
  if (wasUpdated) {
      Logger.log("Task status updated to 'Completed'.");
  }

  // 5. Delete the record
  const wasDeleted = taskSheet.deleteById(newId);
  if (wasDeleted) {
    Logger.log("Task has been deleted.");
  }
}
```

## API Reference

### Initialization

* `SheetORM(sheetName, options)`
    * `sheetName` (`string`): The name of the sheet.
    * `options` (`object`, optional):
        * `spreadsheetId` (`string`): The ID of the spreadsheet. Defaults to the active one.
        * `idField` (`string`): The name of the unique ID column. Defaults to `"id"`.
        * `headerRow` (`number`): The row number where headers are located. Defaults to `1`.

### Create

* `create(recordObject)`: Creates a single new record. Requires the `idField` to be present. Returns `boolean`.
* `createMany(arrayOfRecordObjects)`: Creates multiple new records in a batch. Requires `idField` in each object. Returns the `number` of records created.

### Read

* `findById(id)`: Finds a single record by its unique ID. Returns `object` or `null`.
* `find(conditions)`: Finds the first record matching the `conditions` object. Returns `object` or `null`.
* `findMany(queryOptions)`: Finds all records matching the `queryOptions`. Returns `object[]`. `queryOptions` can include `where`, `select`, `orderBy`, `limit`, and `offset`.
* `getAll()`: Retrieves all records from the sheet. Returns `object[]`.
* `query(customQueryString)`: Executes a raw query with column names in brackets (e.g., `select [Name] where [Age] > 30`). Returns `object[]`.

### Update

* `updateById(id, fieldsToUpdate)`: Updates a single record by its ID. Returns `boolean`.
* `update(conditions, newValues)`: Updates the first record matching the `conditions`. Returns `boolean`.
* `updateMany(conditions, newValues)`: Updates all records matching the `conditions`. Returns the `number` of records updated.

### Delete

* `deleteById(id)`: Deletes a single record by its ID. Returns `boolean`.
* `delete(conditions)`: Deletes the first record matching the `conditions`. Returns `boolean`.
* `deleteMany(conditions)`: Deletes all records matching the `conditions`. Requires a non-empty `conditions` object. Returns the `number` of records deleted.
* `clearData()`: Deletes all data rows, leaving the header row intact. Returns `boolean`.

### License

This project is licensed under the MIT License - see the LICENSE file for details.
