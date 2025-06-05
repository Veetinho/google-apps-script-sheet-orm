/*
 * @license MIT
 * @version 0.0.1
 * @author Vitalii Vykhrystiuk
 * @description ORM-like library for Google Sheets.
 */

/*
 * Configuration options for the SheetORM instance.
 * @typedef {Object} SheetORMOptions
 * @property {string} [spreadsheetId] - The ID of the Google Spreadsheet. If not provided, the active spreadsheet will be used.
 * @property {string} [idField="id"] - The name of the column to be used as the unique identifier for records. This is case-sensitive.
 * @property {number} [headerRow=1] - The row number where the headers are located (1-indexed). Currently, for GViz fetching, this implies the headers are the first row of the data range.
 */

/*
 * Creates an ORM-like interface for a specified Google Sheet.
 * This allows for easier data manipulation (CRUD operations) and querying.
 *
 * @param {string} sheetName - The name of the sheet to interact with.
 * @param {SheetORMOptions} [options={}] - Configuration options for the SheetORM instance.
 * @returns {Object|null} The SheetORM instance with methods to interact with the sheet,
 * or null if initialization fails (e.g., sheet or spreadsheet not found).
 */

function SheetORM(sheetName, options = {}) {
  const DEFAULTS = {
    spreadsheetId: null,
    idField: "id",    // Default unique ID field name
    headerRow: 1,     // Assume headers are on the first row
    lockTime: 30000,  // Lock time in ms
  }

  let _config = { ...DEFAULTS, ...options }
  let _ss = null
  let _ws = null
  let _columnInfoCache = null
  /*
   * _columnInfoCache structure:
   * {
   * headersArray: string[],    // ['Header1', 'Header2', ...]
   * headerToLetter: Object,    // { 'Header1': 'A', 'Header2': 'B', ... }
   * letterToHeader: Object,    // { 'A': 'Header1', 'B': 'Header2', ... }
   * headerToIndex: Object,     // { 'Header1': 0, 'Header2': 1, ... } (0-indexed)
   * columnTypes: Object,       // { 'Header1': 'string', 'A': 'string', ... } (can store by header and letter for convenience)
   * idFieldLetter: string|null // Column letter for the configured idField, e.g., 'A'
   * }
   */

  // --- Initialization Block ---
  try {
    if (typeof sheetName !== 'string' || sheetName.trim() === '') {
      throw new Error("Sheet name must be a non-empty string.")
    }

    if (_config.spreadsheetId) {
      _ss = SpreadsheetApp.openById(_config.spreadsheetId)
    } else {
      _ss = SpreadsheetApp.getActiveSpreadsheet()
      if (!_ss) {
        throw new Error("No active spreadsheet found and no spreadsheetId provided.")
      }
      _config.spreadsheetId = _ss.getId() // Store the resolved ID
    }

    _ws = _ss.getSheetByName(sheetName)
    if (!_ws) {
      throw new Error(`Sheet "${sheetName}" not found in spreadsheet ID "${_config.spreadsheetId}".`)
    }
    // At this point, _ss and _ws are validated and set.
    // console.log(`SheetORM initialized for sheet: "${sheetName}" in spreadsheet: "${_config.spreadsheetId}"`)

  } catch (e) {
    console.log(`SheetORM Initialization Error: ${e.message} (Sheet: "${sheetName}", SpreadsheetID: "${_config.spreadsheetId || 'Active'}")`)
    return null // Indicate failure
  }

  // --- Internal Helper Methods ---

  /**
   * @typedef {Object} GvizColumn
   * @property {string} id - The column letter (e.g., "A", "B").
   * @property {string} label - The header name of the column. Can be empty if the cell is empty.
   * @property {string} type - The data type inferred by Google Sheets for the column (e.g., "string", "number", "boolean", "date", "datetime", "timeofday").
   */

  /**
   * @typedef {Object} ParsedGvizTable
   * @property {GvizColumn[]} cols - An array of column definition objects.
   * @property {Object[]} rows - An array of row data objects (not used by this specific parser).
   * @property {number} [parsedNumHeaders] - Number of header rows parsed by GViz (usually 0 if `headers=N` is used correctly).
   */

  /**
   * @typedef {Object} ParsedGvizResponse
   * @property {string} version - The GViz API version.
   * @property {string} reqId - The request ID.
   * @property {string} status - "ok", "error", or "warning".
   * @property {string[]} [sig] - Signature.
   * @property {ParsedGvizTable} [table] - The data table object, present if status is "ok".
   * @property {Object[]} [errors] - An array of error objects if status is "error".
   * @property {Object[]} [warnings] - An array of warning objects if status is "warning".
   */

  /**
   * Parses the GViz JSONP response text to extract column definitions.
   * This is a specialized parser focusing only on the `table.cols` part for header information.
   * @private
   * @param {string} responseText - The raw response text from a GViz query.
   * @returns {ParsedGvizResponse|null} A parsed GViz response object, or null if the text cannot be parsed into the expected structure.
   */
  function _parseGvizResponseTextForHeaders(responseText) {
    if (!responseText || typeof responseText !== 'string') {
      console.log("_parseGvizResponseTextForHeaders: Input responseText is invalid.")
      return null
    }
    // Remove the GViz JSONP wrapper "google.visualization.Query.setResponse(...);"
    // The actual JSON starts after the first '(' and ends before the last ')'
    const startIndex = responseText.indexOf('(')
    const endIndex = responseText.lastIndexOf(')')

    if (startIndex === -1 || endIndex === -1 || endIndex <= startIndex) {
      console.log("_parseGvizResponseTextForHeaders: Could not find JSONP wrapper in response text.")
      // console.log("Response Text was: " + responseText.substring(0, 200)); // Log snippet for debugging
      return null
    }

    const jsonString = responseText.substring(startIndex + 1, endIndex)

    try {
      const parsed = JSON.parse(jsonString)
      return parsed
    } catch (e) {
      console.log(`_parseGvizResponseTextForHeaders: JSON parsing error: ${e.message}`)
      // console.log("JSON String was: " + jsonString.substring(0, 200)); // Log snippet
      return null
    }
  }

  /**
   * Fetches column information (headers, letters, types) from the sheet using the Google Visualization API (GViz)
   * and populates the internal cache (`_columnInfoCache`). This method is called internally when column
   * information is needed and not yet cached. It assumes that the header row specified in `_config.headerRow`
   * is treated as the source of labels by the GViz query.
   * @private
   * @returns {boolean} True if column info was successfully fetched and cached, false otherwise.
   */
  function _fetchColumnInfo() {
    // If headerRow is not 1, GViz 'headers=1' might not directly map.
    // For now, we assume headerRow:1 in config means the first row of the sheet IS the header.
    // GViz 'headers=N' param means N rows are headers. headers=1 is standard.
    // If _config.headerRow is > 1, this logic would need to be more complex (e.g. query a specific range)
    // For v0.0.1, we keep it simple: GViz `headers=1` implies `_config.headerRow` is 1.
    const query = `SELECT * LIMIT 1` // We only need 1 row to get column structure if headers are used. Or even LIMIT 0 might work for some GViz versions.
    const gvizUrl = `https://docs.google.com/spreadsheets/d/${_config.spreadsheetId}/gviz/tq?sheet=${encodeURIComponent(sheetName)}&tq=${encodeURIComponent(query)}&headers=1`

    try {
      const token = ScriptApp.getOAuthToken()
      const params = {
        method: 'get',
        headers: { 'Authorization': 'Bearer ' + token },
        muteHttpExceptions: true, // We will handle errors manually
      }

      // console.log(`_fetchColumnInfo: Fetching GViz URL: ${gvizUrl}`)
      const response = UrlFetchApp.fetch(gvizUrl, params)
      const responseCode = response.getResponseCode()
      const responseText = response.getContentText()

      if (responseCode !== 200) {
        console.log(`_fetchColumnInfo: GViz request failed. HTTP Status: ${responseCode}. Response: ${responseText.substring(0, 500)}`)
        _columnInfoCache = null // Ensure cache is invalidated
        return false
      }

      const parsedGviz = _parseGvizResponseTextForHeaders(responseText)

      if (!parsedGviz) {
        console.log("_fetchColumnInfo: Failed to parse GViz response for headers.")
        _columnInfoCache = null
        return false
      }

      if (parsedGviz.status === 'error') {
        console.log(`_fetchColumnInfo: GViz API returned an error. Errors: ${JSON.stringify(parsedGviz.errors)}`)
        _columnInfoCache = null
        return false
      }

      if (!parsedGviz.table || !parsedGviz.table.cols || parsedGviz.table.cols.length === 0) {
        console.log("_fetchColumnInfo: GViz response does not contain column definitions (table.cols). Might be an empty sheet or misconfiguration.")
        // If the sheet is truly empty (no headers at all), table.cols might be empty.
        // We should still initialize the cache as empty to avoid refetching constantly.
        _columnInfoCache = {
          headersArray: [],
          headerToLetter: {},
          letterToHeader: {},
          headerToIndex: {},
          columnTypes: {},
          idFieldLetter: null
        }
        return true // Technically successful, but with no columns.
      }

      const cols = parsedGviz.table.cols
      const tempCache = {
        headersArray: [],
        headerToLetter: {},
        letterToHeader: {},
        headerToIndex: {},
        columnTypes: {}, // Store by header name and by letter for convenience
        idFieldLetter: null
      }

      cols.forEach((col, index) => {
        // col.label can be empty if the header cell is empty.
        // For now, we use the label as is. A more robust solution might generate default names
        // or require non-empty headers.
        const headerName = col.label || `_Col${col.id}` // Use column letter if label is empty

        tempCache.headersArray.push(headerName)
        tempCache.headerToLetter[headerName] = col.id // col.id is the letter 'A', 'B', etc.
        tempCache.letterToHeader[col.id] = headerName
        tempCache.headerToIndex[headerName] = index
        tempCache.columnTypes[headerName] = col.type
        tempCache.columnTypes[col.id] = col.type

        if (headerName === _config.idField) {
          tempCache.idFieldLetter = col.id
        }
      })

      if (!tempCache.idFieldLetter && _config.idField) {
        console.log(`_fetchColumnInfo: Warning - Configured idField "${_config.idField}" not found in sheet headers. Operations using ID field may fail.`)
      }

      _columnInfoCache = tempCache
      // console.log(`_fetchColumnInfo: Successfully fetched and cached column info. Headers: ${tempCache.headersArray.join(', ')}`)
      return true

    } catch (e) {
      console.log(`_fetchColumnInfo: Unexpected error during column info fetching: ${e.message} ${e.stack ? ' Stack: ' + e.stack : ''}`)
      _columnInfoCache = null // Invalidate cache on error
      return false
    }
  }

  /* -------------------------------------------------------------------------------------------------------------------------------------- */

  /**
   * Ensures that column information (_columnInfoCache) is populated.
   * If not already cached, it attempts to fetch it using _fetchColumnInfo.
   * @private
   * @returns {Object|null} The column info cache object, or null if fetching failed or cache is empty.
   */
  function _getOrFetchColumnInfo() {
    if (!_columnInfoCache || _columnInfoCache.headersArray.length === 0) { // Check if truly empty
      // Logger.log("_getOrFetchColumnInfo: Cache miss or empty, attempting to fetch column info...")
      if (!_fetchColumnInfo()) { // _fetchColumnInfo populates _columnInfoCache
        Logger.log("_getOrFetchColumnInfo: Failed to fetch column info on demand.")
        return null
      }
    }
    return _columnInfoCache
  }

  /**
   * Executes a given write action (a callback function) within a script lock,
   * flushes changes to the spreadsheet, and handles potential cache clearing.
   * @private
   * @param {Function} actionCallback - The function containing the SpreadsheetApp write operations.
   * This function should throw an error if the operation fails internally
   * to ensure 'success' is not incorrectly set to true.
   * @returns {boolean} True if the action was successful and completed without errors, false otherwise.
   */
  function _executeWriteOperation(actionCallback) {
    const lock = LockService.getScriptLock()
    let success = false
    try {
      // Wait up to 30 seconds for the lock.
      // Consider making timeout configurable in _config if needed.
      if (!lock.tryLock(_config.lockTime)) {
        Logger.log("_executeWriteOperation: Could not obtain lock within 30 seconds.")
        return false // Failed to get lock
      }

      actionCallback() // Execute the sheet modification logic
      SpreadsheetApp.flush() // Ensure all pending changes are written to the spreadsheet immediately

      // Placeholder for clearing a potential GViz data cache after successful write
      // if (_gvizDataCache) { _clearGvizDataCache() }

      success = true
      // Logger.log("_executeWriteOperation: Write operation successful.")
    } catch (e) {
      Logger.log(`_executeWriteOperation: Error during write operation: ${e.message}${e.stack ? ' Stack: ' + e.stack : ''}`)
      success = false; // Ensure success is false on error
    } finally {
      lock.releaseLock()
    }
    return success
  }

  /**
   * Finds row numbers (1-indexed sheet row numbers) that match the given conditions.
   * This helper reads data directly using SpreadsheetApp and is intended for CUD operations.
   * @private
   * @param {Object} conditions - An object where keys are header names and values are the criteria to match.
   * Values are compared as strings.
   * @param {boolean} [findFirst=false] - If true, stops searching and returns after finding the first matching row.
   * @returns {number[]} An array of 1-indexed row numbers that match the conditions. Returns an empty array if no matches are found or if an error occurs.
   */
  function _findRowNumbersByConditions(conditions, findFirst = false) {
    const columnInfo = _getOrFetchColumnInfo()
    if (!columnInfo || columnInfo.headersArray.length === 0) {
      Logger.log("_findRowNumbersByConditions: Column info not available or sheet has no headers. Cannot find rows by conditions.")
      return []
    }

    // Validate conditions structure
    if (typeof conditions !== 'object' || conditions === null || Object.keys(conditions).length === 0) {
      Logger.log("_findRowNumbersByConditions: Conditions parameter must be a non-empty object.")
      return []
    }

    const conditionKeys = Object.keys(conditions)
    const relevantColumnIndices = [] // 0-indexed for data array retrieved from sheet
    const conditionValues = []
    let validConditionFound = false

    for (const key of conditionKeys) {
      if (columnInfo.headerToIndex.hasOwnProperty(key)) {
        relevantColumnIndices.push(columnInfo.headerToIndex[key])
        conditionValues.push(String(conditions[key])) // Compare as strings
        validConditionFound = true
      } else {
        Logger.log(`_findRowNumbersByConditions: Warning - Condition key "${key}" is not a valid header name and will be ignored.`)
      }
    }

    if (!validConditionFound) {
      Logger.log("_findRowNumbersByConditions: No valid header names found in conditions. Cannot find rows.")
      return []
    }

    const firstDataRowIndex = _config.headerRow + 1
    const lastSheetRow = _ws.getLastRow()

    if (lastSheetRow < firstDataRowIndex) {
      // Logger.log("_findRowNumbersByConditions: No data rows exist in the sheet.")
      return [] // No data rows to search
    }

    // Fetch all data for simplicity. Optimization: fetch only relevant columns if sheet is very wide.
    // However, getValues() on a range is often efficient enough.
    const dataRange = _ws.getRange(firstDataRowIndex, 1, lastSheetRow - firstDataRowIndex + 1, columnInfo.headersArray.length)
    const allSheetDataValues = dataRange.getValues()
    const matchingRowNumbers = []

    for (let i = 0; i < allSheetDataValues.length; i++) {
      const currentRowSheetData = allSheetDataValues[i]
      let isMatch = true
      for (let j = 0; j < relevantColumnIndices.length; j++) {
        const columnIndexInSheetData = relevantColumnIndices[j]
        const expectedValueString = conditionValues[j]
        // Ensure value exists and compare as string
        if (String(currentRowSheetData[columnIndexInSheetData]) !== expectedValueString) {
          isMatch = false
          break
        }
      }

      if (isMatch) {
        matchingRowNumbers.push(firstDataRowIndex + i) // Add 1-indexed sheet row number
        if (findFirst) {
          break // Stop if only the first match is needed
        }
      }
    }
    return matchingRowNumbers
  }

  /**
   * Converts a single record object into an array of values, ordered according
   * to the provided orderedHeadersArray. Properties in the object that do not
   * match a header in orderedHeadersArray are ignored. Headers in orderedHeadersArray
   * not found as properties in the recordObject will result in `null` values at that position.
   * @private
   * @param {Object} recordObject - The object representing a single record (e.g., { "Header1": "val1", "Header2": "val2" }).
   * @param {string[]} orderedHeadersArray - An array of header names in the sheet's exact column order (e.g., ["Header1", "Header3", "Header2"]).
   * @returns {any[]} An array of values corresponding to the ordered headers (e.g., ["val1", null, "val2"]).
   * Returns an empty array if input parameters are invalid.
   */
  function _objectToRowArray(recordObject, orderedHeadersArray) {
    if (!recordObject || typeof recordObject !== 'object' || !orderedHeadersArray || !Array.isArray(orderedHeadersArray)) {
      Logger.log("_objectToRowArray: Invalid input parameters. recordObject must be an object, and orderedHeadersArray must be an array.")
      return [] // Or throw an error for critical failure
    }

    const rowArray = []
    for (const header of orderedHeadersArray) {
      if (recordObject.hasOwnProperty(header)) {
        rowArray.push(recordObject[header])
      } else {
        rowArray.push('')
      }
    }
    return rowArray
  }

  /**
   * Converts an array of record objects into a 2D array of values (array of arrays),
   * with values in each inner array ordered according to the provided orderedHeadersArray.
   * This is suitable for methods like `Range.setValues()`.
   * @private
   * @param {Object[]} arrayOfRecordObjects - The array of record objects to convert.
   * @param {string[]} orderedHeadersArray - An array of header names in the sheet's exact column order.
   * @returns {any[][]} A 2D array of values. Returns an empty array if input parameters are invalid or
   * if the input array of objects is empty.
   */
  function _objectsToRowsArray(arrayOfRecordObjects, orderedHeadersArray) {
    if (!Array.isArray(arrayOfRecordObjects) || !orderedHeadersArray || !Array.isArray(orderedHeadersArray)) {
      Logger.log("_objectsToRowsArray: Invalid input parameters. arrayOfRecordObjects and orderedHeadersArray must be arrays.")
      return [] // Or throw an error
    }
    if (arrayOfRecordObjects.length === 0) {
      return [] // No objects to convert
    }

    const rowsArray = []
    for (const recordObject of arrayOfRecordObjects) {
      // Ensure that individual objects are also valid before processing
      if (recordObject && typeof recordObject === 'object') {
        rowsArray.push(_objectToRowArray(recordObject, orderedHeadersArray))
      } else {
        Logger.log(`_objectsToRowsArray: Encountered an invalid item in arrayOfRecordObjects. Item: ${JSON.stringify(recordObject)}. Skipping.`)
      }
    }
    return rowsArray
  }

  /**
   * Builds a Google Query Language (GQL) string from a structured options object.
   * Handles SELECT, WHERE, ORDER BY, LIMIT, and OFFSET clauses.
   * @private
   * @param {Object} [queryOptions={}] - The options for building the query.
   * @param {string[]} [queryOptions.select=null] - Array of header names to select. If null or empty, selects all ("*").
   * @param {Object} [queryOptions.where=null] - Conditions for the WHERE clause (e.g., { "Status": "Active", "Count": 10 }).
   * @param {Object|Object[]} [queryOptions.orderBy=null] - Sorting criteria (e.g., { "Name": "ASC" } or [{ "Category": "ASC" }, { "Date": "DESC" }]).
   * @param {number} [queryOptions.limit=null] - LIMIT clause.
   * @param {number} [queryOptions.offset=null] - OFFSET clause.
   * @returns {string} The constructed GQL query string.
   */
  function _buildGqlQuery(queryOptions = {}) {
    const columnInfo = _getOrFetchColumnInfo() // Ensures we have headerToLetter mapping

    if (!columnInfo) {
      Logger.log("_buildGqlQuery: Column info not available. Cannot build query effectively. Returning 'SELECT *'.")
      return "SELECT *" // Fallback, though this might fail if sheet is empty.
    }

    const { select: selectFields, where: conditions, orderBy, limit, offset } = queryOptions

    // SELECT clause
    let selectClause = "SELECT *"
    if (Array.isArray(selectFields) && selectFields.length > 0) {
      const validSelectColumns = selectFields
        .map(fieldName => columnInfo.headerToLetter[fieldName])
        .filter(Boolean) // Filter out undefined if headerName not found
      if (validSelectColumns.length > 0) {
        selectClause = `SELECT ${validSelectColumns.join(', ')}`
      } else {
        Logger.log(`_buildGqlQuery: None of the specified select fields are valid column headers. Defaulting to SELECT *. Fields: ${JSON.stringify(selectFields)}`)
      }
    }

    // WHERE clause
    let whereClause = ""
    if (conditions && typeof conditions === 'object' && Object.keys(conditions).length > 0) {
      const criteria = []
      for (const fieldName in conditions) {
        if (conditions.hasOwnProperty(fieldName)) {
          const columnLetter = columnInfo.headerToLetter[fieldName]
          if (columnLetter) {
            let value = conditions[fieldName]
            // Basic value quoting for GQL: strings are single-quoted. Numbers are not.
            // More robust type handling based on columnInfo.columnTypes[fieldName] would be better here.
            const columnType = columnInfo.columnTypes[fieldName] // e.g., 'string', 'number', 'boolean', 'date', 'datetime'
            if (columnType === 'string' || columnType === 'date' || columnType === 'datetime' || columnType === 'timeofday') {
              // Escape single quotes within the string value itself
              value = String(value).replace(/'/g, "\\'")
              criteria.push(`${columnLetter} = '${value}'`)
            } else if (columnType === 'number' || columnType === 'boolean') {
              criteria.push(`${columnLetter} = ${value}`)
            } else { // Default to string if type unknown, or if value is explicitly string.
              value = String(value).replace(/'/g, "\\'")
              criteria.push(`${columnLetter} = '${value}'`)
            }
          } else {
            Logger.log(`_buildGqlQuery: WHERE condition field "${fieldName}" is not a valid column header. It will be ignored.`)
          }
        }
      }
      if (criteria.length > 0) {
        whereClause = `WHERE ${criteria.join(' AND ')}`
      }
    }

    // ORDER BY clause
    let orderByClause = ""
    if (orderBy) {
      const orderByArray = Array.isArray(orderBy) ? orderBy : [orderBy]
      const orderByParts = []
      orderByArray.forEach(sortCondition => {
        if (sortCondition && typeof sortCondition === 'object') {
          for (const fieldName in sortCondition) {
            if (sortCondition.hasOwnProperty(fieldName)) {
              const columnLetter = columnInfo.headerToLetter[fieldName]
              if (columnLetter) {
                const direction = String(sortCondition[fieldName]).toUpperCase()
                if (direction === "ASC" || direction === "DESC") {
                  orderByParts.push(`${columnLetter} ${direction}`)
                } else {
                  orderByParts.push(`${columnLetter}`) // Default ASC
                }
              } else {
                Logger.log(`_buildGqlQuery: ORDER BY field "${fieldName}" is not a valid column header. It will be ignored.`)
              }
              break // Handle only one field per sortCondition object for simplicity
            }
          }
        }
      })
      if (orderByParts.length > 0) {
        orderByClause = `ORDER BY ${orderByParts.join(', ')}`
      }
    }

    // LIMIT clause
    let limitClause = ""
    if (typeof limit === 'number' && limit > 0) {
      limitClause = `LIMIT ${limit}`
    }

    // OFFSET clause
    let offsetClause = ""
    if (typeof offset === 'number' && offset > 0) {
      offsetClause = `OFFSET ${offset}`
    }

    const fullQuery = [selectClause, whereClause, orderByClause, limitClause, offsetClause]
      .filter(Boolean) // Remove empty parts
      .join(' ')

    // Logger.log(`_buildGqlQuery: Constructed GQL: ${fullQuery}`);
    return fullQuery
  }

  /**
   * Executes a GViz query using UrlFetchApp and returns the raw response text.
   * @private
   * @param {string} gqlString - The Google Query Language string to execute.
   * @returns {string|null} The raw response text from GViz, or null if the fetch operation fails or returns an error status.
   */
  function _executeGvizQuery(gqlString) {
    if (!gqlString || typeof gqlString !== 'string' || gqlString.trim() === '') {
      Logger.log("_executeGvizQuery: gqlString parameter must be a non-empty string.")
      return null
    }

    // Use _config.headerRow to inform GViz how many rows are headers.
    // GViz 'headers' parameter: Number of rows that are part of the header.
    // If _config.headerRow is 1, then 'headers=1' is appropriate.
    const gvizUrl = `https://docs.google.com/spreadsheets/d/${_config.spreadsheetId}/gviz/tq?sheet=${encodeURIComponent(sheetName)}&tq=${encodeURIComponent(gqlString)}&headers=${_config.headerRow}`

    try {
      const token = ScriptApp.getOAuthToken()
      const params = {
        method: 'get',
        headers: { 'Authorization': 'Bearer ' + token },
        muteHttpExceptions: true, // Handle HTTP errors manually
      };
      // Logger.log(`_executeGvizQuery: Fetching URL: ${gvizUrl}`)
      const response = UrlFetchApp.fetch(gvizUrl, params)
      const responseCode = response.getResponseCode()
      const responseText = response.getContentText()

      if (responseCode !== 200) {
        Logger.log(`_executeGvizQuery: GViz request failed. HTTP Status: ${responseCode}. URL: ${gvizUrl}. Response: ${responseText.substring(0, 500)}`)
        return null
      }
      return responseText
    } catch (e) {
      Logger.log(`_executeGvizQuery: Error during UrlFetchApp.fetch: ${e.message}. URL: ${gvizUrl}`)
      return null
    }
  }

  /**
   * Parses the raw GViz JSONP response text and transforms the data table into an array of JavaScript objects.
   * Handles data type conversions for common types like date, datetime, number, boolean.
   * @private
   * @param {string} responseText - The raw response text from a GViz query.
   * @returns {Object[]|null} An array of record objects, or null if parsing fails or GViz returns an error.
   */
  function _parseGvizDataResponse(responseText) {
    const parsedGviz = _parseGvizResponseTextForHeaders(responseText) // Can reuse the basic JSONP parser

    if (!parsedGviz) {
      Logger.log("_parseGvizDataResponse: Initial parsing of GViz response failed.")
      return null
    }

    if (parsedGviz.status === 'error') {
      Logger.log(`_parseGvizDataResponse: GViz API returned an error. Errors: ${JSON.stringify(parsedGviz.errors)}`)
      return null // Or an empty array if preferred for "no data due to error"
    }

    if (parsedGviz.status === 'warning' && parsedGviz.warnings) {
      Logger.log(`_parseGvizDataResponse: GViz API returned warnings: ${JSON.stringify(parsedGviz.warnings)}`)
    }

    if (!parsedGviz.table || !parsedGviz.table.cols || !parsedGviz.table.rows) {
      // Logger.log("_parseGvizDataResponse: GViz response does not contain a valid table structure (cols or rows missing). This might mean no data matched.")
      return [] // No data or empty table
    }

    if (parsedGviz.table.rows.length === 0) {
      return [] // No rows returned
    }

    const columnInfo = _getOrFetchColumnInfo() // For type information if needed, though GViz provides it
    if (!columnInfo) {
      Logger.log("_parseGvizDataResponse: Could not retrieve column info, cannot reliably map results to headers.")
      return null
    }


    const headersFromGviz = parsedGviz.table.cols
    const resultObjects = []

    parsedGviz.table.rows.forEach(row => {
      const record = {}
      if (row.c) { // 'c' is the array of cells for the row
        row.c.forEach((cell, colIndex) => {
          const headerDef = headersFromGviz[colIndex]
          if (headerDef) {
            const headerLabel = headerDef.label // This is the header name from the sheet
            let cellValue = null

            if (cell !== null && cell.v !== undefined && cell.v !== null) { // cell.v is the actual value
              cellValue = cell.v
              const gvizType = headerDef.type

              // Type conversion based on GViz type
              if ((gvizType === 'date' || gvizType === 'datetime') && typeof cellValue === 'string' && cellValue.startsWith('Date(')) {
                // Format is "Date(Year,Month,Day,Hour,Minute,Second)" - Month is 0-11
                const dateArgs = cellValue.substring(5, cellValue.length - 1).split(',')
                try {
                  cellValue = new Date(
                    parseInt(dateArgs[0], 10),
                    parseInt(dateArgs[1], 10), // Month is 0-indexed
                    parseInt(dateArgs[2], 10),
                    dateArgs.length > 3 ? parseInt(dateArgs[3], 10) : 0,
                    dateArgs.length > 4 ? parseInt(dateArgs[4], 10) : 0,
                    dateArgs.length > 5 ? parseInt(dateArgs[5], 10) : 0,
                    dateArgs.length > 6 ? parseInt(dateArgs[6], 10) : 0
                  )
                } catch (dateErr) {
                  Logger.log(`_parseGvizDataResponse: Error parsing GViz date string "${cell.v}". Using formatted value or original. Error: ${dateErr.message}`)
                  cellValue = cell.f || cell.v // Fallback to formatted string or original value
                }

              } else if (gvizType === 'timeofday' && Array.isArray(cellValue)) {
                // Format is [hour, minute, second, milliseconds]
                // Convert to a simple HH:MM:SS string for now, or handle as needed
                cellValue = cellValue.slice(0, 3).map(n => String(n).padStart(2, '0')).join(':')
              }
              // GViz usually returns numbers as numbers and booleans as booleans for `cell.v`
              // No explicit conversion needed for `number` or `boolean` if `cell.v` is already correct type.
            }
            // Use the header label from GViz as the key. This ensures it matches what the user sees.
            // If label is empty, one might fall back to col.id (letter) or a generated name.
            // Our _fetchColumnInfo already handles empty labels with `_Col${col.id}`.
            // Here, we should trust headerDef.label from the gviz response.
            if (headerLabel !== "") {
              record[headerLabel] = cellValue
            } else if (columnInfo.letterToHeader[headerDef.id]) {
              // Fallback to our cached header name if GViz label is empty but we have one
              record[columnInfo.letterToHeader[headerDef.id]] = cellValue
            }
            // Else, if headerLabel is empty and no cached name, data for this column might be inaccessible by name.
          }
        })
      }
      resultObjects.push(record)
    })

    return resultObjects
  }

  /**
   * Translates a custom query string, where column names are enclosed in square brackets (e.g., "[Name]"),
   * into a valid Google Query Language (GQL) string using actual column letters (e.g., "A").
   * This function uses the cached column information to perform the translation.
   *
   * @private
   * @param {string} customQueryString - The custom query string to translate.
   * @returns {string} The translated GQL query string.
   * @throws {Error} If a bracketed column name in the query string does not match any known header,
   * an error is thrown indicating the invalid column.
   */
  function _translateCustomQueryToGql(customQueryString) {
    const columnInfo = _getOrFetchColumnInfo()
    if (!columnInfo || !columnInfo.headerToLetter || Object.keys(columnInfo.headerToLetter).length === 0) {
      throw new Error("Cannot translate query: Column information is not available or sheet has no headers. Ensure the sheet has headers and is accessible.")
    }

    // Regular expression to find all occurrences of [columnName]
    // It captures the content inside the brackets (columnName)
    const bracketedColumnRegex = /\[([^\]]+)\]/g // Matches [anything_not_a_closing_bracket]

    const translatedQuery = customQueryString.replace(bracketedColumnRegex, (_match, headerName) => {
      // `match` is the full string that matched the regex, e.g., "[Name]"
      // `headerName` is the captured group, i.e., "Name"

      const columnLetter = columnInfo.headerToLetter[headerName]
      if (columnLetter === undefined) {
        // If the headerName extracted from brackets is not found in our cache,
        // it means it's an invalid column name for this sheet.
        throw new Error(`Invalid column name "[${headerName}]" found in query string. It does not match any known header in the sheet.`)
      }
      return columnLetter // Replace "[Name]" with "A"
    })

    return translatedQuery
  }

  // --- Public API definition ---
  const publicApi = {
    /**
     * Clears all data rows from the sheet, leaving the header row intact.
     * The header row is determined by the 'headerRow' option (default is 1).
     * @memberof SheetORM
     * @returns {boolean} True if data was cleared successfully or if there was no data to clear, false otherwise.
     */
    clearData: function () {
      return _executeWriteOperation(() => {
        const firstDataRowToDelete = _config.headerRow + 1
        const lastSheetRowWithData = _ws.getLastRow()

        if (lastSheetRowWithData >= firstDataRowToDelete) {
          _ws.deleteRows(firstDataRowToDelete, lastSheetRowWithData - firstDataRowToDelete + 1)
          // Logger.log(`clearData: Deleted rows from ${firstDataRowToDelete} to ${lastSheetRowWithData}.`)
        } else {
          // Logger.log("clearData: No data rows to delete.")
        }
        // Placeholder for clearing any GViz data cache
        // if (_gvizDataCache) { _clearGvizDataCache(); }
      })
    },

    /**
     * Deletes a single record from the sheet based on its unique ID.
     * The ID field is determined by the 'idField' option in the SheetORM configuration.
     * @memberof SheetORM
     * @param {string|number} id - The unique ID of the record to delete. Must not be null, undefined, or empty.
     * @returns {boolean} True if the record was found and deleted successfully, false otherwise (e.g., ID not found, ID field not configured, or delete operation failed).
     */
    deleteById: function (id) {
      const columnInfo = _getOrFetchColumnInfo()
      if (!columnInfo) {
        Logger.log("deleteById: Failed to get column information. Cannot proceed.")
        return false
      }
      if (!columnInfo.idFieldLetter) {
        Logger.log(`deleteById: ID field "${_config.idField}" is not configured or not found in sheet headers. Cannot delete by ID.`)
        return false
      }
      if (id === null || id === undefined || (typeof id === 'string' && id.trim() === '')) {
        Logger.log("deleteById: Provided ID is null, undefined, or empty. Deletion aborted.")
        return false
      }

      const conditions = {}
      conditions[columnInfo.letterToHeader[columnInfo.idFieldLetter]] = id

      const rowNumbersToDelete = _findRowNumbersByConditions(conditions, true)

      if (rowNumbersToDelete.length === 0) {
        // Logger.log(`deleteById: Record with ID "${id}" (in field "${_config.idField}") not found.`)
        return false // Record not found
      }

      const rowToDelete = rowNumbersToDelete[0] // Get the specific row number

      return _executeWriteOperation(() => {
        _ws.deleteRow(rowToDelete)
        // Logger.log(`deleteById: Successfully deleted record with ID "${id}" from row ${rowToDelete}.`);
        // Placeholder for clearing any GViz data cache
        // if (_gvizDataCache) { _clearGvizDataCache() }
      })
    },

    /**
     * Deletes the first record found that matches the given conditions.
     * @memberof SheetORM
     * @param {Object} conditions - An object where keys are header names and values are the criteria to match.
     * This object must not be empty and must contain at least one valid header key.
     * @returns {boolean} True if a record matching the conditions was found and deleted successfully, false otherwise.
     */
    delete: function (conditions) {
      // _getOrFetchColumnInfo() will be called by _findRowNumbersByConditions
      // Basic validation for conditions is also handled by _findRowNumbersByConditions
      const rowNumbersToDelete = _findRowNumbersByConditions(conditions, true) // findFirst = true

      if (rowNumbersToDelete.length === 0) {
        // Logger.log(`delete: No record found matching conditions: ${JSON.stringify(conditions)}.`)
        return false // No record found or invalid conditions
      }

      const rowToDelete = rowNumbersToDelete[0] // Get the specific row number

      return _executeWriteOperation(() => {
        _ws.deleteRow(rowToDelete)
        // Logger.log(`delete: Successfully deleted first record matching conditions from row ${rowToDelete}.`)
        // Placeholder for clearing any GViz data cache
        // if (_gvizDataCache) { _clearGvizDataCache() }
      })
    },

    /**
     * Deletes all records from the sheet that match the given conditions.
     * To prevent accidental mass deletion of all sheet data, the 'conditions' object
     * must not be empty and must result in at least one valid condition key that exists as a header.
     * For clearing all data rows (except headers) without conditions, use the `clearData()` method.
     * @memberof SheetORM
     * @param {Object} conditions - An object where keys are header names and values are the criteria to match.
     * @returns {number} The number of records deleted. Returns 0 if no records matched the conditions,
     * if conditions were invalid, or if the delete operation failed.
     */
    deleteMany: function (conditions) {
      const rowNumbersToDelete = _findRowNumbersByConditions(conditions, false) // findFirst = false

      if (rowNumbersToDelete.length === 0) {
        // Logger.log(`deleteMany: No records found matching conditions: ${JSON.stringify(conditions)}.`)
        return 0 // No records found or invalid conditions
      }

      // Sort row numbers in descending order to delete from bottom-up,
      // preventing issues with shifting row indices during deletion.
      rowNumbersToDelete.sort((a, b) => b - a)

      const countToDelete = rowNumbersToDelete.length
      const success = _executeWriteOperation(() => {
        for (const rowNum of rowNumbersToDelete) {
          _ws.deleteRow(rowNum)
        }
        // Logger.log(`deleteMany: Successfully deleted ${countToDelete} records matching conditions.`)
        // Placeholder for clearing any GViz data cache
        // if (_gvizDataCache) { _clearGvizDataCache() }
      })

      return success ? countToDelete : 0
    },

    /**
     * Creates a single new record (appends a new row) in the sheet.
     * The record object MUST contain a non-empty value for the field specified
     * as 'idField' in the SheetORM configuration (defaults to "id").
     * The order of values in the new row is determined by the order of headers in the sheet.
     * @memberof SheetORM
     * @param {Object} recordObject - An object where keys are header names (column names)
     * and values are the data for the new record.
     * @returns {boolean} True if the record was created successfully, false otherwise (e.g., missing/empty ID,
     * invalid input, column info not available, or write operation failed).
     */
    create: function (recordObject) {
      if (typeof recordObject !== 'object' || recordObject === null || Object.keys(recordObject).length === 0) {
        Logger.log("create: recordObject parameter must be a non-empty object.")
        return false
      }

      const columnInfo = _getOrFetchColumnInfo();
      if (!columnInfo || !columnInfo.headersArray || columnInfo.headersArray.length === 0) {
        Logger.log("create: Column info not available or sheet has no headers. Cannot create record.")
        return false;
      }

      const idFieldName = _config.idField
      if (!recordObject.hasOwnProperty(idFieldName)) {
        Logger.log(`create: Record object is missing the required ID field "${idFieldName}". Record not created.`)
        return false
      }
      const idValue = recordObject[idFieldName];
      if (idValue === null || idValue === undefined || String(idValue).trim() === "") {
        Logger.log(`create: The ID field "${idFieldName}" must not be null, undefined, or an empty string. Received: "${idValue}". Record not created.`)
        return false
      }

      const rowArray = _objectToRowArray(recordObject, columnInfo.headersArray)

      if (rowArray.length !== columnInfo.headersArray.length) {
        Logger.log("create: Failed to convert recordObject to a valid row array (length mismatch). Record not created.")
        return false
      }

      return _executeWriteOperation(() => {
        _ws.appendRow(rowArray)
        // Logger.log(`create: Successfully appended new record. ID [${idFieldName}]: "${idValue}"`)
      })
    },

    /**
     * Creates multiple new records in the sheet in a single batch operation.
     * Each record object in the array MUST contain a non-empty value for the field specified
     * as 'idField' in the SheetORM configuration (defaults to "id").
     * If any record object fails this validation, the entire batch operation will be aborted.
     * @memberof SheetORM
     * @param {Object[]} arrayOfRecordObjects - An array of record objects.
     * @returns {number} The number of records successfully created. Returns 0 if the input array is empty,
     * if any record fails ID validation, if column info is unavailable,
     * if conversion to rows fails, or if the write operation fails.
     */
    createMany: function (arrayOfRecordObjects) {
      if (!Array.isArray(arrayOfRecordObjects) || arrayOfRecordObjects.length === 0) {
        Logger.log("createMany: arrayOfRecordObjects parameter must be a non-empty array.")
        return 0
      }

      const columnInfo = _getOrFetchColumnInfo()
      if (!columnInfo || !columnInfo.headersArray || columnInfo.headersArray.length === 0) {
        Logger.log("createMany: Column info not available or sheet has no headers. Cannot create records.")
        return 0
      }

      const idFieldName = _config.idField
      for (let i = 0; i < arrayOfRecordObjects.length; i++) {
        const recordObject = arrayOfRecordObjects[i]
        if (!recordObject || typeof recordObject !== 'object') {
          Logger.log(`createMany: Item at index ${i} is not a valid object. Batch creation aborted.`)
          return 0
        }
        if (!recordObject.hasOwnProperty(idFieldName)) {
          Logger.log(`createMany: Record object at index ${i} is missing the required ID field "${idFieldName}". Batch creation aborted.`)
          return 0
        }
        const idValue = recordObject[idFieldName]
        if (idValue === null || idValue === undefined || String(idValue).trim() === "") {
          Logger.log(`createMany: The ID field "${idFieldName}" in record object at index ${i} must not be null, undefined, or an empty string. Received: "${idValue}". Batch creation aborted.`)
          return 0
        }
      }

      const rowsDataArray = _objectsToRowsArray(arrayOfRecordObjects, columnInfo.headersArray)

      if (rowsDataArray.length === 0 && arrayOfRecordObjects.length > 0) {
        Logger.log("createMany: No valid rows could be converted from arrayOfRecordObjects (this might indicate all input objects were problematic despite passing initial ID check, or an issue in _objectsToRowsArray).")
        return 0
      }
      if (rowsDataArray.length > 0 && (rowsDataArray[0].length !== columnInfo.headersArray.length)) {
        Logger.log("createMany: Converted rows do not match header column count. Batch creation aborted.");
        return 0
      }
      if (rowsDataArray.length !== arrayOfRecordObjects.length) {
        Logger.log("createMany: The number of converted rows does not match the number of input objects. This might indicate some objects were skipped due to errors in _objectsToRowsArray. Batch creation aborted for safety.")
        return 0
      }


      const numRecordsToAdd = rowsDataArray.length
      const numColumns = columnInfo.headersArray.length

      const success = _executeWriteOperation(() => {
        const lastRowWithContent = _ws.getLastRow()
        const startInsertRow = lastRowWithContent + 1
        _ws.getRange(startInsertRow, 1, numRecordsToAdd, numColumns).setValues(rowsDataArray)
        // Logger.log(`createMany: Attempted to add ${numRecordsToAdd} new records starting at row ${startInsertRow}.`)
      })

      return success ? numRecordsToAdd : 0
    },

    /**
     * Updates a single record in the sheet identified by its unique ID.
     * Only the fields provided in the 'fieldsToUpdate' object will be modified.
     * The ID field itself (as configured in 'idField') cannot be updated using this method.
     * @memberof SheetORM
     * @param {string|number} id - The unique ID of the record to update. Must not be null, undefined, or empty.
     * @param {Object} fieldsToUpdate - An object where keys are header names (column names)
     * and values are the new data for those fields. Must be a non-empty object.
     * @returns {boolean} True if the record was found and updated successfully.
     * Returns false if the record was not found, if 'id' or 'fieldsToUpdate' are invalid,
     * if 'fieldsToUpdate' attempts to change the ID field, if no valid fields to update are provided,
     * or if the write operation fails.
     */
    updateById: function (id, fieldsToUpdate) {
      if (id === null || id === undefined || (typeof id === 'string' && id.trim() === '')) {
        Logger.log("updateById: Provided ID is null, undefined, or empty. Update aborted.")
        return false
      }
      if (typeof fieldsToUpdate !== 'object' || fieldsToUpdate === null || Object.keys(fieldsToUpdate).length === 0) {
        Logger.log("updateById: fieldsToUpdate parameter must be a non-empty object. Update aborted.")
        return false
      }

      const columnInfo = _getOrFetchColumnInfo()
      if (!columnInfo) {
        Logger.log("updateById: Failed to get column information. Cannot proceed with update.")
        return false
      }
      if (!columnInfo.idFieldLetter) { // Check if idField was successfully mapped
        Logger.log(`updateById: ID field "${_config.idField}" is not configured properly or not found in sheet headers. Cannot update by ID.`)
        return false
      }

      const idFieldNameFromCache = columnInfo.letterToHeader[columnInfo.idFieldLetter]
      if (fieldsToUpdate.hasOwnProperty(idFieldNameFromCache)) {
        Logger.log(`updateById: The ID field "${idFieldNameFromCache}" cannot be part of the fieldsToUpdate object. Update aborted.`)
        return false
      }

      const conditions = {}
      conditions[idFieldNameFromCache] = id

      const rowNumbersToUpdate = _findRowNumbersByConditions(conditions, true) // findFirst = true

      if (rowNumbersToUpdate.length === 0) {
        // Logger.log(`updateById: Record with ID "${id}" (field: "${idFieldNameFromCache}") not found.`)
        return false // Record not found
      }
      const rowToUpdate = rowNumbersToUpdate[0] // Get the specific row number

      // Collect valid updates to perform
      const updatesToPerform = [] // Stores {row, col (1-indexed), value}
      let hasValidUpdates = false
      for (const fieldName in fieldsToUpdate) {
        if (fieldsToUpdate.hasOwnProperty(fieldName)) {
          if (columnInfo.headerToIndex.hasOwnProperty(fieldName)) {
            const columnIndex = columnInfo.headerToIndex[fieldName] + 1 // +1 for 1-indexed getRange
            updatesToPerform.push({
              row: rowToUpdate,
              col: columnIndex,
              value: fieldsToUpdate[fieldName]
            })
            hasValidUpdates = true
          } else {
            Logger.log(`updateById: Warning - Field "${fieldName}" in fieldsToUpdate is not a valid header name and will be ignored.`)
          }
        }
      }

      if (!hasValidUpdates) {
        Logger.log("updateById: No valid fields to update were provided (after filtering against known headers). No update performed.")
        return false // No actual changes to make
      }

      return _executeWriteOperation(() => {
        for (const update of updatesToPerform) {
          _ws.getRange(update.row, update.col).setValue(update.value)
        }
        // Logger.log(`updateById: Successfully updated record with ID "${id}" at row ${rowToUpdate}. ${updatesToPerform.length} fields modified.`)
      })
    },

    /**
     * Updates the first record found in the sheet that matches the given conditions.
     * Only the fields provided in the 'newValues' object will be modified.
     * The ID field (as configured in 'idField') cannot be updated using this method.
     * @memberof SheetORM
     * @param {Object} conditions - An object where keys are header names and values are the criteria to match.
     * Must be a non-empty object and contain at least one valid header key.
     * @param {Object} newValues - An object containing the fields and their new values to apply to the found record.
     * Must be a non-empty object.
     * @returns {boolean} True if a record was found and updated successfully.
     * Returns false if no record matched, if parameters were invalid, if 'newValues' attempts to change the ID field,
     * if no valid fields to update are provided, or if the write operation failed.
     */
    update: function (conditions, newValues) {
      if (typeof conditions !== 'object' || conditions === null || Object.keys(conditions).length === 0) {
        Logger.log("update: 'conditions' parameter must be a non-empty object.")
        return false
      }
      if (typeof newValues !== 'object' || newValues === null || Object.keys(newValues).length === 0) {
        Logger.log("update: 'newValues' parameter must be a non-empty object.")
        return false
      }

      const columnInfo = _getOrFetchColumnInfo();
      if (!columnInfo) {
        Logger.log("update: Failed to get column information. Cannot proceed.")
        return false
      }

      if (columnInfo.idFieldLetter && newValues.hasOwnProperty(columnInfo.letterToHeader[columnInfo.idFieldLetter])) {
        Logger.log(`update: The ID field "${columnInfo.letterToHeader[columnInfo.idFieldLetter]}" cannot be updated. Please remove it from the newValues object. Update aborted.`)
        return false
      }

      const rowNumbersToUpdate = _findRowNumbersByConditions(conditions, true) // findFirst = true

      if (rowNumbersToUpdate.length === 0) {
        // Logger.log(`update: No record found matching conditions: ${JSON.stringify(conditions)}.`)
        return false // No record found or invalid conditions
      }
      const rowToUpdate = rowNumbersToUpdate[0]

      const updatesToPerform = []
      let hasValidUpdates = false
      for (const fieldName in newValues) {
        if (newValues.hasOwnProperty(fieldName)) {
          if (columnInfo.headerToIndex.hasOwnProperty(fieldName)) {
            const columnIndex = columnInfo.headerToIndex[fieldName] + 1
            updatesToPerform.push({
              row: rowToUpdate,
              col: columnIndex,
              value: newValues[fieldName]
            })
            hasValidUpdates = true
          } else {
            Logger.log(`update: Warning - Field "${fieldName}" in newValues is not a valid header name and will be ignored.`)
          }
        }
      }

      if (!hasValidUpdates) {
        Logger.log("update: No valid fields to update were provided in newValues (after filtering against known headers). No update performed.")
        return false
      }

      return _executeWriteOperation(() => {
        for (const update of updatesToPerform) {
          _ws.getRange(update.row, update.col).setValue(update.value)
        }
        // Logger.log(`update: Successfully updated first record matching conditions at row ${rowToUpdate}. ${updatesToPerform.length} fields modified.`)
      })
    },

    /**
     * Updates all records in the sheet that match the given conditions.
     * Only the fields provided in the 'newValues' object will be modified in each matching record.
     * The ID field (as configured in 'idField') cannot be updated using this method.
     * @memberof SheetORM
     * @param {Object} conditions - An object where keys are header names and values are the criteria to match.
     * Must be a non-empty object and contain at least one valid header key.
     * @param {Object} newValues - An object containing the fields and their new values to apply to all matching records.
     * Must be a non-empty object.
     * @returns {number} The number of records updated. Returns 0 if no records matched,
     * if parameters were invalid, if 'newValues' attempts to change the ID field,
     * if no valid fields to update are provided, or if the write operation failed.
     */
    updateMany: function (conditions, newValues) {
      if (typeof conditions !== 'object' || conditions === null || Object.keys(conditions).length === 0) {
        Logger.log("updateMany: 'conditions' parameter must be a non-empty object.")
        return 0
      }
      if (typeof newValues !== 'object' || newValues === null || Object.keys(newValues).length === 0) {
        Logger.log("updateMany: 'newValues' parameter must be a non-empty object.")
        return 0
      }

      const columnInfo = _getOrFetchColumnInfo()
      if (!columnInfo) {
        Logger.log("updateMany: Failed to get column information. Cannot proceed.")
        return 0
      }

      if (columnInfo.idFieldLetter && newValues.hasOwnProperty(columnInfo.letterToHeader[columnInfo.idFieldLetter])) {
        Logger.log(`updateMany: The ID field "${columnInfo.letterToHeader[columnInfo.idFieldLetter]}" cannot be updated. Please remove it from the newValues object. Update aborted.`)
        return 0
      }

      const rowNumbersToUpdate = _findRowNumbersByConditions(conditions, false) // findFirst = false

      if (rowNumbersToUpdate.length === 0) {
        // Logger.log(`updateMany: No records found matching conditions: ${JSON.stringify(conditions)}.`)
        return 0 // No records found or invalid conditions
      }

      // Prepare the list of field updates (column index and value) once
      const fieldUpdatesToApply = []
      let hasValidFieldsToUpdate = false
      for (const fieldName in newValues) {
        if (newValues.hasOwnProperty(fieldName)) {
          if (columnInfo.headerToIndex.hasOwnProperty(fieldName)) {
            fieldUpdatesToApply.push({
              col: columnInfo.headerToIndex[fieldName] + 1,
              value: newValues[fieldName]
            })
            hasValidFieldsToUpdate = true
          } else {
            Logger.log(`updateMany: Warning - Field "${fieldName}" in newValues is not a valid header name and will be ignored.`)
          }
        }
      }

      if (!hasValidFieldsToUpdate) {
        Logger.log("updateMany: No valid fields to update were provided in newValues (after filtering against known headers). No records updated.")
        return 0
      }

      const countToUpdate = rowNumbersToUpdate.length
      const success = _executeWriteOperation(() => {
        for (const rowNum of rowNumbersToUpdate) {
          for (const update of fieldUpdatesToApply) {
            _ws.getRange(rowNum, update.col).setValue(update.value)
          }
        }
        // Logger.log(`updateMany: Successfully processed updates for ${countToUpdate} records matching conditions. ${fieldUpdatesToApply.length} fields targeted per record.`)
      })

      return success ? countToUpdate : 0
    },

    /**
     * Retrieves a single record by its unique ID.
     * The ID field is determined by the 'idField' option in the SheetORM configuration.
     * @memberof SheetORM
     * @param {string|number} id - The unique ID of the record to retrieve. Must not be null, undefined or empty.
     * @returns {Object|null} The record object if found, null otherwise (e.g., not found, ID invalid,
     * ID field not configured, or query error).
     */
    findById: function (id) {
      if (id === null || id === undefined || (typeof id === 'string' && id.trim() === '')) {
        Logger.log("findById: Provided ID is null, undefined, or empty. Cannot find record.")
        return null
      }

      const columnInfo = _getOrFetchColumnInfo()
      if (!columnInfo) {
        Logger.log("findById: Failed to get column information. Cannot proceed.")
        return null
      }
      if (!columnInfo.idFieldLetter) {
        Logger.log(`findById: ID field "${_config.idField}" is not configured or not found in sheet headers. Cannot find by ID.`)
        return null
      }

      const idFieldName = columnInfo.letterToHeader[columnInfo.idFieldLetter]
      const queryOptions = {
        where: { [idFieldName]: id },
        limit: 1
      }
      const gqlString = _buildGqlQuery(queryOptions)
      const responseText = _executeGvizQuery(gqlString)

      if (responseText === null) {
        return null // Error occurred
      }
      const results = _parseGvizDataResponse(responseText)
      return results && results.length > 0 ? results[0] : null
    },

    /**
     * Retrieves the first single record that matches the given conditions.
     * @memberof SheetORM
     * @param {Object} conditions - An object where keys are header names and values are the criteria to match.
     * Must be a non-empty object and contain at least one valid header key.
     * @returns {Object|null} The first matching record object if found, null otherwise (e.g., no match,
     * invalid conditions, or query error).
     */
    find: function (conditions) {
      if (typeof conditions !== 'object' || conditions === null || Object.keys(conditions).length === 0) {
        Logger.log("find: 'conditions' parameter must be a non-empty object.")
        return null
      }
      // To ensure _buildGqlQuery can validate condition keys against actual headers
      const columnInfo = _getOrFetchColumnInfo()
      if (!columnInfo) {
        Logger.log("find: Column info not available. Cannot proceed.")
        return null
      }
      // Check if any condition key is actually a header
      let hasValidConditionKey = false
      for (const key in conditions) {
        if (columnInfo.headerToLetter.hasOwnProperty(key)) {
          hasValidConditionKey = true
          break
        }
      }
      if (!hasValidConditionKey) {
        Logger.log(`find: None of the keys in 'conditions' object match known headers. Conditions: ${JSON.stringify(conditions)}`)
        return null
      }


      const queryOptions = {
        where: conditions,
        limit: 1
      }
      const gqlString = _buildGqlQuery(queryOptions)
      const responseText = _executeGvizQuery(gqlString)

      if (responseText === null) {
        return null
      }
      const results = _parseGvizDataResponse(responseText)
      return results && results.length > 0 ? results[0] : null
    },

    /**
     * Retrieves multiple records from the sheet that match the given query options
     * (including conditions for filtering, selection of specific columns, sorting, limit, and offset).
     * @memberof SheetORM
     * @param {Object} [queryOptions={}] - An object to specify query details:
     * @param {Object} [queryOptions.where=null] - Field-value pairs for filtering records.
     * @param {string[]} [queryOptions.select=null] - Array of header names for columns to retrieve. If null/empty, selects all.
     * @param {Object|Object[]} [queryOptions.orderBy=null] - Sorting criteria (e.g., { "HeaderName": "ASC" }).
     * @param {number} [queryOptions.limit=null] - Maximum number of records to return.
     * @param {number} [queryOptions.offset=null] - Number of records to skip (for pagination).
     * @returns {Object[]|null} An array of record objects that match the criteria. Returns an empty array if no records match.
     * Returns null if a query execution error occurs.
     */
    findMany: function (queryOptions = {}) {
      const gqlString = _buildGqlQuery(queryOptions)
      const responseText = _executeGvizQuery(gqlString)
      if (responseText === null) {
        return null // Error during query execution
      }
      return _parseGvizDataResponse(responseText)
    },

    /**
     * Executes a custom query string against the sheet, providing maximum flexibility.
     * The query string uses a simplified syntax where column names are enclosed in square brackets [].
     * These bracketed names are automatically translated into the correct Google Query Language (GQL)
     * column letters (A, B, C...) before the query is sent to the Google Visualization API.
     * The rest of the syntax should follow the Google Query Language specification for data manipulation.
     *
     * @memberof SheetORM
     * @param {string} customQueryString - The custom query string to execute.
     * Must be a non-empty string.
     * @example
     * // Simple select all from specific columns
     * const data = orm.query("select [Name], [Email]");
     *
     * // Query with conditions (WHERE clause)
     * const activeUsers = orm.query("select * where [Age] > 25 and [Status] = 'Active'");
     *
     * // Complex query with OR, LIKE, and sorting
     * const managers = orm.query("select [Name], [Position] where [Salary] >= 50000 and ([Position] like '%Manager%' or [Position] starts with 'Lead') order by [Name] asc limit 10");
     *
     * @returns {Object[]|null} An array of record objects that match the query.
     * Returns an empty array if no records match the query.
     * Returns null if the input query string is invalid, if a column name in the query cannot be translated,
     * or if a query execution error occurs.
     */
    query: function (customQueryString) {
      if (!customQueryString || typeof customQueryString !== 'string' || customQueryString.trim() === '') {
        Logger.log("query: customQueryString parameter must be a non-empty string.")
        return null
      }

      let gqlString = null
      try {
        // Attempt to translate the user's custom query string into a valid GQL string.
        gqlString = _translateCustomQueryToGql(customQueryString)
      } catch (e) {
        // Catch errors thrown by _translateCustomQueryToGql (e.g., invalid column name)
        Logger.log(`query: Error during query translation: ${e.message}`)
        return null // Return null to indicate translation failure
      }

      // Execute the translated GQL query against the Google Visualization API.
      const responseText = _executeGvizQuery(gqlString)
      if (responseText === null) {
        // _executeGvizQuery will log errors, so just return null here.
        return null
      }

      // Parse the raw GViz response and transform it into an array of JavaScript objects.
      return _parseGvizDataResponse(responseText)
    }
  }

  // --- Return the public API ---
  return publicApi
}
