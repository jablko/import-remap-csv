/** @import {CellValue, DetailedCellError} from "hyperformula" */
/**
 * https://github.com/handsontable/hyperformula/discussions/1506
 *
 * @import {PluginFunctionType} from "hyperformula/typings/interpreter/plugin/FunctionPlugin"
 */

function onOpen() {
  const ui = SpreadsheetApp.getUi();
  ui.createAddonMenu().addItem("Start", "start").addToUi();
}

function start() {
  const ui = SpreadsheetApp.getUi();
  const html =
    HtmlService.createHtmlOutputFromFile("Index").setTitle("Import/remap CSV");
  ui.showSidebar(html);
}

/**
 * Do the things ... parse CSV, compare it to the existing spreadsheet and add
 * and/or modify transactions.
 *
 * @param {string} csv
 * @param {unknown} crosswalk
 * @param {boolean} preview
 */
function doImport(csv, crosswalk, preview) {
  const sheet = /** @type {GoogleAppsScript.Spreadsheet.Sheet} */ (
    ss.getSheetByName("Transactions")
  );
  if (sheet === null) {
    throw new Error("Transactions sheet not found");
  }
  const tHeader = getHeader(sheet);
  /**
   * Transaction IDs -> (multiple) zero-based spreadsheet row numbers.
   *
   * @type {ReadonlyMap<unknown, readonly number[]>}
   */
  const byID = invertEntries(
    getColumn(
      sheet,
      /** @type {number} */ (tHeader.get("Transaction ID")) + 1,
    ).entries(),
  );
  /** @type {[(readonly string[])?, ...(readonly string[][])]} */
  const [csvHeaderRow, ...csvData] = Utilities.parseCsv(csv);
  if (csvHeaderRow === undefined || csvHeaderRow.length < 1) {
    throw new Error("Empty CSV/headers");
  }
  /**
   * @type {{
   *   headerRow: readonly string[];
   *   data: readonly (CellValue | Date)[][];
   *   crosswalks: readonly unknown[];
   * }}
   */
  const { headerRow, data, crosswalks } = ({ crosswalk } = remap(
    csvHeaderRow,
    csvData,
    crosswalk,
  ));
  /**
   * Header names -> (single) CSV column numbers. We exploit it being the same
   * size as the number of CSV headers/columns.
   */
  const header = new Map(headerRow.map((name, j) => [name, j]));
  if (header.size !== headerRow.length) {
    throw new Error(
      crosswalk === "Tiller (bypass remap)"
        ? "Duplicate CSV headers"
        : "Duplicate crosswalk headers",
    );
  }
  // Prepare CSV
  prepDate(header, data);
  prepAmount(header, data);
  prepDescription(header, data);
  prepID(header, data, tHeader);
  autoCat(header, data);
  /**
   * For duplicate IDs, map CSV transactions -> spreadsheet transactions by ID
   * occurrence number.
   *
   * @type {Record<string, number>}
   */
  const counter = {};
  /**
   * CSV -> spreadsheet --- row numbers -> row numbers.
   *
   * @type {readonly (number | undefined)[]}
   */
  const rMap = data.map((row) => {
    const id = row[/** @type {number} */ (header.get("Transaction ID"))];
    //return byID.get(id)?.[(counter[/** @type {never} */ (id)] ??= 0)++];
    counter[/** @type {never} */ (id)] =
      counter[/** @type {never} */ (id)] ?? 0;
    return byID.get(id)?.[counter[/** @type {never} */ (id)]++];
  });
  /**
   * CSV -> spreadsheet --- column numbers -> column numbers.
   *
   * @type {[...(readonly (number | undefined)[])]}
   */
  const [...cMap] = /** @type {never} */ (
    header.keys().map((name) => tHeader.get(name))
  );
  // The smallest rectangle that covers the imported rows and columns. The
  // Spreadsheet service operates on one rectangle at a time, it is not
  // asynchronous and doing an unbounded number of operations is slow, so
  // operate on one, minimum rectangle for theoretical performance? The Sheets
  // API batchGet()/batchUpdate() are overkill?
  const rMin = Math.min(...rMap.filter((i) => i !== undefined));
  const rMax = Math.max(...rMap.filter((i) => i !== undefined));
  const cMin = Math.min(...cMap.filter((j) => j !== undefined));
  const cMax = Math.max(...cMap.filter((j) => j !== undefined));
  const addCMin = Math.min(cMin, tHeader.get("Date Added") ?? Math.min());
  // Partition transactions to add and ones to modify
  /** @type {(CellValue | Date)[][]} */
  const addData = [];
  /** @type {(readonly (CellValue | Date)[])[]} */
  let modifyData = [];
  for (const [i, row] of data.entries()) {
    const overlay = [];
    // AutoCat can make row sparse
    for (const [j, value] of Object.entries(row)) {
      overlay[/** @type {never} */ (cMap[j])] = value;
    }
    if (rMap[i] === undefined) {
      overlay[/** @type {never} */ (tHeader.get("Date Added"))] = now;
      addData.push(overlay.slice(addCMin));
    } else {
      modifyData[rMap[i] - rMin] = overlay.slice(cMin);
    }
  }
  /**
   * Existing spreadsheet transactions for comparison.
   *
   * @type {(CellValue | Date)[][]}
   */
  let tData;
  if (modifyData.length > 0) {
    const range = sheet.getRange(
      1 + rMin + 1,
      cMin + 1,
      rMax - rMin + 1,
      cMax - cMin + 1,
    );
    Logger.log(`Get range ${range.getA1Notation()} for comparison`);
    tData = range.getValues();
  }
  if (preview) {
    /** Compare to the existing spreadsheet. */
    let nModify = 0;
    for (const [i, row] of Object.entries(modifyData)) {
      nModify += /** @type {never} */ (
        row.some((value, j) => !equals(value, tData[i][j]))
      );
    }
    return {
      crosswalk,
      crosswalks,
      nAdd: addData.length,
      nModify,
      nTotal: data.length,
      header: headerRow.map((name, j) => ({
        name,
        found: tHeader.get(name) !== undefined,
        // Errors are in the first row because that is where the array formulas
        // are
        summary:
          /** @type {Partial<DetailedCellError>?} */ (data[0]?.[j])?.message !==
          undefined
            ? data[0][j]
            : summarize(addData, /** @type {never} */ (cMap[j]) - addCMin),
      })),
    };
  }
  add();
  modify();
  sheet.sort(/** @type {number} */ (tHeader.get("Date")) + 1, false);

  function add() {
    // No transactions to add or disjoint headers
    if (addData.length < 1 || cMax < cMin) {
      return;
    }
    const nGrow = sheet.getLastRow() + addData.length - sheet.getMaxRows();
    if (nGrow > 0) {
      Logger.log(`Grow spreadsheet by ${nGrow} rows`);
      sheet.insertRowsAfter(sheet.getMaxRows(), nGrow);
    }
    // Rightmost column can be sparse if an AutoCat rule does not match any rows
    const range = sheet.getRange(
      sheet.getLastRow() + 1,
      addCMin + 1,
      addData.length,
      Math.max(...addData.map((row) => row.length)),
    );
    Logger.log(`Add transactions to range ${range.getA1Notation()}`);
    noDataValidationSetValues(range, addData);
  }

  function modify() {
    // Compare to the existing spreadsheet and shrink the rectangle that we
    // modify to cover just the rows and columns that differ
    const modifyRMin = Number(
      Object.keys(modifyData).find((i) =>
        modifyData[i].some((value, j) => !equals(value, tData[i][j])),
      ),
    );
    // All are already identical
    if (Number.isNaN(modifyRMin)) {
      return;
    }
    const modifyRMax = Number(
      Object.keys(modifyData).findLast((i) =>
        modifyData[i].some((value, j) => !equals(value, tData[i][j])),
      ),
    );
    modifyData = modifyData.slice(modifyRMin, modifyRMax + 1);
    tData = tData.slice(modifyRMin, modifyRMax + 1);
    const modifyCMin = Math.min(
      ...modifyData
        .map((row, i) =>
          Object.keys(row).find((j) => !equals(row[j], tData[i][j])),
        )
        .filter((j) => j !== undefined),
    );
    const modifyCMax = Math.max(
      ...modifyData
        .map((row, i) =>
          Object.keys(row).findLast((j) => !equals(row[j], tData[i][j])),
        )
        .filter((j) => j !== undefined),
    );
    tData = tData.map((row) => row.slice(modifyCMin, modifyCMax + 1));
    // Merge CSV and the existing spreadsheet. `modifyData` is sparse.
    for (const [i, row] of Object.entries(modifyData)) {
      Object.assign(tData[i], row.slice(modifyCMin, modifyCMax + 1));
    }
    const range = sheet.getRange(
      1 + rMin + modifyRMin + 1,
      cMin + modifyCMin + 1,
      modifyRMax - modifyRMin + 1,
      modifyCMax - modifyCMin + 1,
    );
    Logger.log(`Modify transactions in range ${range.getA1Notation()}`);
    noDataValidationSetValues(range, tData);
  }
}

const ss = SpreadsheetApp.getActiveSpreadsheet();

/**
 * Header names -> (last) zero-based spreadsheet column numbers. The first
 * header is normally a GoogleAppsScript.Spreadsheet.CellImage.
 *
 * @param {GoogleAppsScript.Spreadsheet.Sheet} sheet
 */
function getHeader(sheet) {
  const range = sheet.getRange(1, 1, 1, sheet.getLastColumn());
  Logger.log(`Get headers from range ${range.getA1Notation()}`);
  return /** @type {ReadonlyMap<unknown, number>} */ (
    new Map(
      range
        .getValues()
        .flat()
        .map((name, j) => [name, j]),
    )
  );
}

/**
 * The number of rows in the range must be at least 1.
 *
 * @param {GoogleAppsScript.Spreadsheet.Sheet} sheet
 * @param {number} j
 */
function getColumn(sheet, j) {
  const m = sheet.getLastRow() - 1;
  if (m < 1) {
    return [];
  }
  const range = sheet.getRange(2, j, m);
  Logger.log(`Get column from range ${range.getA1Notation()}`);
  return /** @type {readonly unknown[]} */ (range.getValues().flat());
}

/**
 * Values -> (multiple) keys.
 *
 * @template K, V
 * @param {Iterable<readonly [K, V]>} entries
 */
function invertEntries(entries) {
  /** @type {[Iterable<K>, readonly V[]] | []} */
  const [keys, values] = /** @type {never} */ (zip(...entries));
  return Map.groupBy(
    keys ?? [],
    (unused, i) => /** @type {NonNullable<typeof values>} */ (values)[i],
  );
}

/**
 * @template {readonly Iterable<unknown>[]} A
 * @param {A} iterables
 */
function zip(...iterables) {
  const [first, ...rest] = iterables.map((iterable) => Iterator.from(iterable));
  return (
    /**
     * `zip(Iterable<T1>, Iterable<T2>)` is `IteratorObject<[T1, T2]>` and
     * `zip(...Iterable<[T1, T2]>)` should be `IteratorObject<T1[] | T2[]>` but
     * actually it is `IteratorObject<(T1 | T2)[]>`. Makes no difference for
     * now.
     *
     * @type {IteratorObject<{
     *   [K in keyof A]: A[K] extends Iterable<infer T> ? T : never;
     * }>}
     */ (
      first === undefined
        ? Iterator.from([])
        : first.map((value) => [
            value,
            ...rest.map((iterator) => iterator.next().value),
          ])
    )
  );
}

/**
 * Remap CSV via the crosswalks sheet.
 *
 * @param {readonly string[]} headerRow
 * @param {readonly string[][]} data
 * @param {unknown} crosswalk
 */
function remap(headerRow, data, crosswalk) {
  const sheet = /** @type {GoogleAppsScript.Spreadsheet.Sheet} */ (
    ss.getSheetByName("Crosswalks")
  );
  if (sheet === null) {
    Logger.log("Crosswalks sheet not found");
    return {
      headerRow,
      data,
      crosswalk: "Tiller (bypass remap)",
      crosswalks: [],
    };
  }
  const range = sheet.getDataRange();
  Logger.log(`Get crosswalks from range ${range.getA1Notation()}`);
  /** @type {[readonly string[], ...(readonly (readonly unknown[])[])]} */
  const [crosswalksHeaderRow, ...crosswalksData] = /** @type {never} */ (
    getFormulasOrValues(range)
  );
  if (crosswalksHeaderRow === undefined) {
    throw new Error("Empty crosswalks sheet");
  }
  const crosswalks = crosswalksData.map((row) => row[0]);
  if (crosswalk === "Tiller (bypass remap)") {
    Logger.log("Bypass remap");
    return { headerRow, data, crosswalk: "Tiller (bypass remap)", crosswalks };
  }
  registerParseDate();
  const options = {
    licenseKey: "gpl-v3",
    useArrayArithmetic: true,
    dateFormats: [],
  };
  const hf = HyperFormula.buildFromArray(/** @type {never} */ (data), options);
  /**
   * Top left corner of the result. Load the CSV data and the crosswalk formulas
   * side by side in a single HyperFormula sheet.
   */
  const start = { sheet: 0, row: 0, col: headerRow.length };
  return crosswalk !== "" ? viaCrosswalk() : detectCrosswalk();

  function viaCrosswalk() {
    Logger.log(`Remap via ${crosswalk} crosswalk`);
    const i = crosswalks.indexOf(crosswalk);
    const { names, formulas, literals } = filterAndPartition(
      zip(crosswalksHeaderRow, crosswalksData[i]),
    );
    if (names === undefined) {
      throw new Error("Empty crosswalk row/headers");
    }
    hf.setCellContents(start, [
      formulas.map((formula) => bind(formula, headerRow)),
    ]);
    const result = getResult(data.length, formulas.length);
    for (const row of result) {
      Object.assign(row, literals);
    }
    /** HyperFormula columns back -> crosswalks columns. */
    const sourceMap = zip(crosswalksHeaderRow, crosswalksData[i]).flatMap(
      ([name, value], j) => (name !== "" && value !== "" ? [j] : []),
    );
    for (const [value, j] of zip(result[0], sourceMap)) {
      linkToSource(value, sheet, i, j);
    }
    return { headerRow: names, data: result, crosswalk, crosswalks };
  }

  /**
   * Find the first crosswalk without errors. Reading just the first row is
   * faster than reading the whole result of each crosswalk.
   */
  function detectCrosswalk() {
    for (const crosswalksRow of crosswalksData) {
      const { names, formulas, literals } = filterAndPartition(
        zip(crosswalksHeaderRow, crosswalksRow),
      );
      if (names === undefined) {
        continue;
      }
      hf.setCellContents(start, [
        formulas.map((formula) => bind(formula, headerRow)),
      ]);
      if (
        getResult(1, formulas.length)
          .flat()
          .some(
            (value) =>
              /** @type {Partial<DetailedCellError>?} */ (value)?.message !==
              undefined,
          )
      ) {
        continue;
      }
      Logger.log(`Detected ${crosswalksRow[0]} crosswalk`);
      const result = getResult(data.length, formulas.length);
      for (const row of result) {
        Object.assign(row, literals);
      }
      return {
        headerRow: names,
        data: result,
        crosswalk: crosswalksRow[0],
        crosswalks,
      };
    }
    Logger.log("No compatible crosswalk detected");
    return { headerRow, data, crosswalk: "Tiller (bypass remap)", crosswalks };
  }

  /**
   * @param {number} m
   * @param {number} n
   */
  function getResult(m, n) {
    const end = { sheet: 0, row: m - 1, col: start.col + n - 1 };
    return hf.getRangeValues({ start, end });
  }
}

/**
 * Support either formulas or literals.
 *
 * @param {GoogleAppsScript.Spreadsheet.Range} range
 */
function getFormulasOrValues(range) {
  /** @type {readonly unknown[][]} */
  const formulasOrValues = range.getValues();
  // Not sparse too bad
  const formulas = range.getFormulas();
  for (const [i, row] of formulasOrValues.entries()) {
    for (const [j, formula] of formulas[i].entries()) {
      if (formula !== "") {
        row[j] = formula;
      }
    }
  }
  return formulasOrValues;
}

/**
 * Expose Utilities.parseDate() as a custom function, for formats JS does not
 * understand. To use the spreadsheet time zone call the function with just the
 * two remaining arguments, `date` and `format`.
 */
function registerParseDate() {
  class ParseDatePlugin extends HyperFormula.FunctionPlugin {
    /** @type {PluginFunctionType} */
    parseDate(ast, state) {
      return this.runFunction(
        ast.args,
        state,
        this.metadata("PARSEDATE"),
        /** @param {[string, string, string?]} args */
        (...args) => {
          let date,
            timeZone = ssTimeZone,
            format;
          switch (/** @type {2 | -1} */ (args.lastIndexOf(undefined))) {
            case 2:
              [date, format] = args;
              break;
            case -1:
              [date, timeZone, format] = args;
              break;
          }
          try {
            return Utilities.parseDate(
              date,
              timeZone,
              /** @type {NonNullable<typeof format>} */ (format),
            ).toJSON();
          } catch (e) {
            return /** @type {never} */ (e);
          }
        },
      );
    }
  }
  ParseDatePlugin.implementedFunctions = {
    PARSEDATE: {
      method: "parseDate",
      parameters: [
        { argumentType: HyperFormula.FunctionArgumentType.STRING },
        { argumentType: HyperFormula.FunctionArgumentType.STRING },
        {
          argumentType: HyperFormula.FunctionArgumentType.STRING,
          optionalArg: true,
        },
      ],
    },
  };
  HyperFormula.registerFunctionPlugin(ParseDatePlugin, translations);
}

/** HyperFormula's default language. */
const translations = { enGB: { PARSEDATE: "PARSEDATE" } };

/**
 * Ignore empty headers/cells and partition formulas and literals.
 *
 * @param {IteratorObject<readonly [string, unknown]>} entries
 */
function filterAndPartition(entries) {
  /** @type {[readonly string[], readonly unknown[]] | []} */
  const [names, values] = /** @type {never} */ (
    zip(...entries.filter(([name, value]) => name !== "" && value !== ""))
  );
  if (values === undefined) {
    return {};
  }
  /** @type {string[]} */
  const formulas = [];
  /** @type {unknown[]} */
  const literals = [];
  for (const [j, value] of values.entries()) {
    (typeof value === "string" && value.startsWith("=") ? formulas : literals)[
      j
    ] = value;
  }
  return { names, formulas, literals };
}

/**
 * For some reason HyperFormula treats named ranges differently from literal
 * ones, so inline them statically. It is less intelligent than using named
 * expressions but good enough for now? and maybe HyperFormula will get there?
 * https://github.com/handsontable/hyperformula/issues/1533
 *
 * @param {string} formula
 * @param {readonly string[]} headerRow
 */
function bind(formula, headerRow) {
  for (const [j, name] of headerRow.entries()) {
    const pattern = `(?<!")\\b${name.replace(/[^0-9A-Z_]/gi, "")}\\b`;
    let notation = "";
    // In positional notation 10 (radix) comes after radix - 1 but in this
    // bijective one AA comes after Z (radix).
    // https://en.wikipedia.org/wiki/Bijective_numeration#The_bijective_base-26_system
    for (let q = j + 1; q !== 0; q = Math.trunc((q - 1) / 26)) {
      notation = String.fromCharCode(65 + ((q - 1) % 26)) + notation;
    }
    formula = formula.replace(
      new RegExp(pattern, "gi"),
      `{${notation}:${notation}}`,
    );
  }
  return formula;
}

/**
 * Link from errors back to the formulas they came from.
 *
 * @param {CellValue} value
 * @param {GoogleAppsScript.Spreadsheet.Sheet} sheet
 * @param {number} i
 * @param {number} j
 */
function linkToSource(value, sheet, i, j) {
  if (
    /** @type {Partial<DetailedCellError>?} */ (value)?.message === undefined
  ) {
    return;
  }
  const range = sheet.getRange(1 + i + 1, j + 1);
  /** @type {{ url?: unknown }} */ (value).url =
    `${ss.getUrl()}?gid=${sheet.getSheetId()}&range=${range.getA1Notation()}`;
}

/**
 * Parse the date and derive the month and week columns.
 *
 * @param {Map<string, number>} header
 * @param {readonly (CellValue | Date)[][]} data
 */
function prepDate(header, data) {
  const jDate = header.get("Date");
  if (
    jDate === undefined ||
    /** @type {Partial<DetailedCellError>?} */ (data[0]?.[jDate])?.message !==
      undefined
  ) {
    return;
  }
  // Nulls, booleans, numbers and numeric strings are all invalid
  for (const row of data) {
    const value = row[jDate];
    row[jDate] = new Date(
      /** @type {never} */ (isNaN(/** @type {never} */ (value)) ? value : NaN),
    );
  }
  /**
   * JS interprets yyyy-MM-dd strings using UTC and other strings without
   * explicit time zones using the script time zone, which may or may not equal
   * the spreadsheet time zone. We do not detect the case it encountered
   * (explicit, yyyy-MM-dd or other) but if dates are all midnight, local time
   * or UTC, then guess they were all date-only and reinterpret them using the
   * spreadsheet time zone. The ultimate solution is probably Temporal objects
   * when Apps Script implements that, or a third-party library until then?
   */
  const from = data.every((row) =>
    isMidnight(/** @type {never} */ (row[jDate])),
  )
    ? scriptTimeZone
    : data.every((row) => isMidnightUTC(/** @type {never} */ (row[jDate])))
      ? "GMT"
      : null;
  if (from !== null) {
    Logger.log(
      `Interpret date-only strings using the spreadsheet time zone ${ssTimeZone}`,
    );
    for (const row of data) {
      row[jDate] = addTimeZoneOffset(
        /** @type {never} */ (row[jDate]),
        ssTimeZone,
        from,
      );
    }
  }
  const jMonth = header.get("Month") ?? header.size;
  header.set("Month", jMonth);
  const jWeek = header.get("Week") ?? header.size;
  header.set("Week", jWeek);
  for (const row of data) {
    /** Spreadsheet time zone calendar day. */
    const local = addTimeZoneOffset(
      /** @type {never} */ (row[jDate]),
      scriptTimeZone,
      ssTimeZone,
    );
    row[jMonth] = addTimeZoneOffset(
      new Date(local.getFullYear(), local.getMonth()),
      ssTimeZone,
      scriptTimeZone,
    );
    row[jWeek] = addTimeZoneOffset(
      new Date(
        local.getFullYear(),
        local.getMonth(),
        local.getDate() - local.getDay(),
      ),
      ssTimeZone,
      scriptTimeZone,
    );
  }
}

/** @param {Date} date */
function isMidnight(date) {
  return (
    date.getHours() === 0 &&
    date.getMinutes() === 0 &&
    date.getSeconds() === 0 &&
    date.getMilliseconds() === 0
  );
}

/** @param {Date} date */
function isMidnightUTC(date) {
  return (
    date.getUTCHours() === 0 &&
    date.getUTCMinutes() === 0 &&
    date.getUTCSeconds() === 0 &&
    date.getUTCMilliseconds() === 0
  );
}

const scriptTimeZone = Session.getScriptTimeZone();
const ssTimeZone = ss.getSpreadsheetTimeZone();

/**
 * Add to `date` the difference between the `to` and `from` time zones (`to`
 * minus `from`). `from` defaults to GMT, i.e. zero.
 *
 * The implementation formats `date` using the `from` time zone, then
 * reinterprets that representation using the `to` time zone. Using the JS date
 * time string format is arbitrary, equivalent formats make no difference.
 *
 * @param {Date} date
 * @param {string} to
 */
function addTimeZoneOffset(date, to, from = "GMT") {
  return Utilities.parseDate(
    Utilities.formatDate(date, from, dateTimeStringFormat),
    to,
    dateTimeStringFormat,
  );
}

/** https://tc39.es/ecma262/multipage/numbers-and-dates.html#sec-date-time-string-format */
const dateTimeStringFormat = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";

/**
 * Normalize the amount like the Spreadsheet service does, for equals() and
 * prepID().
 *
 * @param {ReadonlyMap<string, number>} header
 * @param {readonly (CellValue | Date)[][]} data
 */
function prepAmount(header, data) {
  const jAmount = header.get("Amount");
  if (
    jAmount === undefined ||
    /** @type {Partial<DetailedCellError>?} */ (data[0]?.[jAmount])?.message !==
      undefined
  ) {
    return;
  }
  for (const row of data) {
    row[jAmount] = Number(String(row[jAmount]).replace(/[$,]/g, ""));
  }
}

/**
 * Derive the description and full description from either column.
 *
 * @param {Map<string, number>} header
 * @param {readonly (CellValue | Date)[][]} data
 */
function prepDescription(header, data) {
  if (
    (header.get("Description") === undefined) ===
    (header.get("Full Description") === undefined)
  ) {
    return;
  }
  const j = /** @type {number} */ (
    header.get("Full Description") ?? header.get("Description")
  );
  if (
    /** @type {Partial<DetailedCellError>?} */ (data[0]?.[j])?.message !==
    undefined
  ) {
    return;
  }
  const jDescription = header.get("Description") ?? header.size;
  header.set("Description", jDescription);
  const jFullDescription = header.get("Full Description") ?? header.size;
  header.set("Full Description", jFullDescription);
  for (const row of data) {
    row[jDescription] = toDescription(String((row[jFullDescription] = row[j])));
  }
}

/**
 * ?Yodlee redacts numbers and Tiller title cases the description.
 *
 * @param {string} fullDescription
 */
function toDescription(fullDescription) {
  return fullDescription
    .replace(/ {2,}/g, " ")
    .replace(/[0-9](?=[- 0-9]+[0-9]{3})/g, "X")
    .replace(/[^- ]{2}[^ ]*/g, (match, start) => {
      switch (match.toUpperCase()) {
        case "A":
        case "AN":
        case "AND":
        case "AT":
        case "BUT":
        case "BY":
        case "FOR":
        case "IN":
        case "NOR":
        case "OF":
        case "OFF":
        case "ON":
        case "OR":
        case "OUT":
        case "SO":
        case "THE":
        case "TO":
        case "UP":
        case "VIA":
        case "YET":
          if (start === 0) {
            break;
          }
          return match.toLowerCase();
        case "AB":
        case "AK":
        case "AL":
        case "AR":
        case "AZ":
        case "BC":
        case "CA":
        case "CO":
        case "CT":
        case "DC":
        case "DE":
        case "FL":
        case "GA":
        case "HI":
        case "IA":
        case "ID":
        case "IL":
        case "IN":
        case "KS":
        case "KY":
        case "LA":
        case "MA":
        case "MB":
        case "MD":
        case "ME":
        case "MI":
        case "MN":
        case "MO":
        case "MS":
        case "MT":
        case "NB":
        case "NC":
        case "ND":
        case "NE":
        case "NH":
        case "NJ":
        case "NL":
        case "NM":
        case "NS":
        case "NT":
        case "NU":
        case "NV":
        case "NY":
        //case "OH":
        case "OK":
        case "ON":
        case "OR":
        case "PA":
        case "PEI":
        case "QC":
        case "RI":
        case "SC":
        case "SD":
        case "SK":
        case "TN":
        case "TX":
        case "UT":
        case "VA":
        case "VT":
        case "WA":
        case "WI":
        case "WV":
        case "WY":
        case "YT":
          return match;
      }
      const [first, ...rest] = match;
      return first + rest.join("").toLowerCase();
    })
    .replace(/X{3,}/gi, "x");
}

/**
 * Give transactions content IDs so we do not import them again, at least.
 *
 * @param {Map<string, number>} header
 * @param {Iterable<(CellValue | Date)[]>} data
 * @param {ReadonlyMap<unknown, number>} tHeader
 */
function prepID(header, data, tHeader) {
  if (header.get("Transaction ID") !== undefined) {
    return;
  }
  Logger.log("Give transactions content IDs");
  const jID = header.size;
  header.set("Transaction ID", jID);
  /**
   * Exclude the derived month and week and include only the normalized
   * description.
   */
  const [...canonical] = header.keys().filter((name) => {
    switch (name) {
      case "Month":
      case "Week":
      case "Full Description":
        return false;
    }
    return tHeader.get(name) !== undefined;
  });
  canonical.sort();
  /** Do not give identical transactions duplicate IDs. */
  const groups =
    /** @type {Readonly<Record<string, readonly (CellValue | Date)[][]>>} */ (
      Object.groupBy(data, (row) => {
        /** @type {Record<string, CellValue | Date>} */
        const o = {};
        for (const name of canonical) {
          const value = row[/** @type {number} */ (header.get(name))];
          if (value !== "") {
            o[name] = value;
          }
        }
        return JSON.stringify(o).toUpperCase();
      })
    );
  for (const [s, group] of Object.entries(groups)) {
    for (const [i, row] of group.entries()) {
      const digest = Utilities.computeDigest(
        Utilities.DigestAlgorithm.SHA_256,
        s + i,
      );
      // RFC 6920 Naming Things with Hashes
      row[jID] =
        `ni:///sha-256;${Utilities.base64EncodeWebSafe(digest).replaceAll("=", "")}`;
    }
  }
}

/**
 * @callback Predicate
 * @param {CellValue | Date} a
 * @param {unknown} b
 * @returns {boolean}
 */

/**
 * Run AutoCat on import.
 *
 * @param {Map<string, number>} header
 * @param {Iterable<(CellValue | Date)[]>} data
 */
function autoCat(header, data) {
  const sheet = ss.getSheetByName("AutoCat");
  if (sheet === null) {
    Logger.log("AutoCat sheet not found");
    return;
  }
  const range = sheet.getDataRange();
  Logger.log(`Get AutoCat rules from range ${range.getA1Notation()}`);
  /** @type {[(readonly string[])?, ...(readonly (readonly unknown[])[])]} */
  const [autoCatHeaderRow, ...autoCatData] = range.getValues();
  if (autoCatHeaderRow === undefined) {
    throw new Error("Empty AutoCat sheet");
  }
  /** @type Predicate[] */
  const conjunction = [];
  /**
   * AutoCat columns -> CSV columns.
   *
   * @type {readonly (number | undefined)[]}
   */
  const autoCatMap = autoCatHeaderRow.map((name, j) => {
    for (const [suffix, predicate] of predicates) {
      if (name.toUpperCase().endsWith(suffix)) {
        conjunction[j] = predicate;
        return header.get(name.slice(0, -suffix.length));
      }
    }
    const y = header.get(name) ?? header.size;
    header.set(name, y);
    return y;
  });
  Logger.log("Run AutoCat on import");
  for (const row of data) {
    for (const autoCatRow of autoCatData) {
      if (match(row, autoCatRow)) {
        assign(row, autoCatRow);
        break;
      }
    }
  }

  /**
   * @param {readonly (CellValue | Date)[]} row
   * @param {readonly unknown[]} autoCatRow
   */
  function match(row, autoCatRow) {
    return conjunction.every(
      (predicate, j) =>
        autoCatRow[j] === "" ||
        predicate(row[/** @type {never} */ (autoCatMap[j])], autoCatRow[j]),
    );
  }

  /**
   * @param {(CellValue | Date)[]} row
   * @param {readonly unknown[]} autoCatRow
   */
  function assign(row, autoCatRow) {
    for (const [j, value] of autoCatRow.entries()) {
      // Columns either have a suffix/predicate or they are replacement
      // values
      if (value !== "" && conjunction[j] === undefined) {
        row[/** @type {never} */ (autoCatMap[j])] = /** @type {never} */ (
          value
        );
      }
    }
  }
}

/** @type {readonly (readonly [string, Predicate])[]} */
const predicates = [
  [
    " CONTAINS",
    (a, b) => String(a).toUpperCase().includes(String(b).toUpperCase()),
  ],
  [
    " EQUALS",
    (a, b) =>
      equals(
        typeof a !== "string" ? a : a.toUpperCase(),
        String(b).toUpperCase(),
      ),
  ],
  [
    " STARTS WITH",
    (a, b) => String(a).toUpperCase().startsWith(String(b).toUpperCase()),
  ],
  [
    " ENDS WITH",
    (a, b) => String(a).toUpperCase().endsWith(String(b).toUpperCase()),
  ],
  [
    " REGEX",
    (a, b) =>
      new RegExp(/** @type {never} */ (b), "i").test(/** @type {never} */ (a)),
  ],
  [" MIN", (a, b) => /** @type {never} */ (a) >= /** @type {never} */ (b)],
  [" MAX", (a, b) => /** @type {never} */ (a) <= /** @type {never} */ (b)],
];

const now = new Date();

/**
 * Compare Date objects as serial numbers (of days since the epoch) and compare
 * strings and numbers loosely.
 *
 * @param {CellValue | Date} a
 * @param {CellValue | Date} b
 */
function equals(a, b) {
  return toSerialNumber(a) == toSerialNumber(b);
}

/**
 * Sheets dates are serial numbers of days since December 30, 1899 ignoring
 * daylight saving time (e.g. using UTC).
 * https://developers.google.com/workspace/sheets/api/guides/formats#about_date_time_values
 *
 * @template T
 * @param {T} date
 */
function toSerialNumber(date) {
  return !(date instanceof Date)
    ? /** @type {Exclude<T, Date>} */ (date)
    : (/** @type {never} */ (addTimeZoneOffset(date, "GMT", ssTimeZone)) -
        epoch) /
        msPerDay;
}

/** Sheets epoch is December 30, 1899. */
const epoch = Date.UTC(1899, 11, 30);
const msPerSecond = 1000;
const msPerMinute = 60 * msPerSecond;
const msPerHour = 60 * msPerMinute;
const msPerDay = 24 * msPerHour;

/**
 * Summarize constant-valued, numeric and date columns. Depends on earlier
 * number and date preparation/normalization.
 *
 * @param {readonly (readonly (CellValue | Date)[])[]} data
 * @param {number} j
 */
function summarize(data, j) {
  const [first, ...rest] = data;
  if (first === undefined || Number.isNaN(j)) {
    return;
  }
  if (first[j] instanceof Date) {
    const column = data.map((row) => row[j]);
    try {
      return dateFormatter.formatRange(
        Math.min(.../** @type {Iterable<never>} */ (column)),
        Math.max(.../** @type {Iterable<never>} */ (column)),
      );
    } catch {
      // Intl.DateTimeFormat chokes on invalid dates
    }
  }
  const constant = String(first[j]);
  if (rest.every((row) => String(row[j]) === constant)) {
    return constant;
  }
  if (typeof first[j] === "number") {
    let sum = 0;
    for (const row of data) {
      sum += /** @type {never} */ (row[j]);
    }
    return `Sum: ${numberFormatter.format(sum)}`;
  }
}

const dateFormatter = new Intl.DateTimeFormat(undefined, {
  timeZone: ssTimeZone,
  dateStyle: "medium",
});
const numberFormatter = new Intl.NumberFormat();

/**
 * Suppress data validation errors.
 *
 * @param {GoogleAppsScript.Spreadsheet.Range} range
 * @param {(CellValue | Date)[][]} values
 */
function noDataValidationSetValues(range, values) {
  const rules = range.getDataValidations();
  range.clearDataValidations();
  try {
    range.setValues(values);
  } finally {
    range.setDataValidations(rules);
  }
}

/**
 * Expose activate() to google.script.run (client-side).
 *
 * @param {number} id
 * @param {string} notation
 */
function activate(id, notation) {
  const sheet = ss.getSheetById(id);
  if (sheet === null) {
    throw new Error(`Sheet ${id} not found`);
  }
  sheet.getRange(notation).activate();
}
