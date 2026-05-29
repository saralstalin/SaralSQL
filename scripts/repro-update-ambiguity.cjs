/* eslint-disable no-console */
const { indexText, tableTypesByName, tablesByName, definitions, referencesIndex, aliasesByUri, columnsByTable } = require("../out/server/src/definitions.js");
const { parseSql } = require("../out/server/src/sql-parser.js");
const { getLineStarts } = require("../out/server/src/text-utils.js");
const { collectAmbiguousColumnDiagnostics } = require("../out/server/src/diagnostic-helpers.js");

function resetState() {
  aliasesByUri.clear();
  definitions.clear();
  referencesIndex.clear();
  columnsByTable.clear();
  tablesByName.clear();
  tableTypesByName.clear();
}

function runCase(name, schemaSql, querySql) {
  resetState();
  indexText(`file:///repro/${name}/schema.sql`, schemaSql);
  indexText(`file:///repro/${name}/query.sql`, querySql);
  const parsed = parseSql(querySql);
  const diagnostics = collectAmbiguousColumnDiagnostics(
    parsed,
    getLineStarts(querySql),
    tablesByName,
    tableTypesByName,
    "SaralSQL"
  );
  const ambiguous = diagnostics.filter((d) => String(d.code).toUpperCase() === "LSP003");
  const facAmbiguous = ambiguous.filter((d) => String(d.message).includes("FacilityCode"));
  return { ambiguous, facAmbiguous };
}

const schemaSql = `
CREATE TYPE dbo.FacilityCodeListType AS TABLE (FacilityCode VARCHAR(20));
CREATE TABLE dbo.ItemFacilityDispositionAvailability (
  ItemNumber VARCHAR(60),
  FacilityCode VARCHAR(20),
  DispositionId INT,
  ReservationQty INT,
  CommitQty INT,
  IsActive BIT
);
CREATE TABLE dbo.ItemFacilityDispositionReservationsCommits (
  ItemNumber VARCHAR(60),
  FacilityCode VARCHAR(20),
  DispositionId INT,
  Qty INT,
  IsCommitted BIT,
  IsActive BIT
);
`;

const cases = [
  {
    name: "update-target-bare-where",
    sql: `
DECLARE @Facilities dbo.FacilityCodeListType;
DECLARE @ItemNumber VARCHAR(60), @DispositionId INT, @TargetFacilityCode VARCHAR(20);
UPDATE dbo.ItemFacilityDispositionAvailability
SET ReservationQty = 1
WHERE ItemNumber = @ItemNumber
  AND FacilityCode = @TargetFacilityCode
  AND DispositionId = @DispositionId
  AND IsActive = 1;
`
  },
  {
    name: "update-target-subqueries-bare-where",
    sql: `
DECLARE @Facilities dbo.FacilityCodeListType;
DECLARE @ItemNumber VARCHAR(60), @DispositionId INT, @TargetFacilityCode VARCHAR(20);
UPDATE dbo.ItemFacilityDispositionAvailability
SET ReservationQty = (
      SELECT ISNULL(SUM(Qty), 0)
      FROM dbo.ItemFacilityDispositionReservationsCommits
      WHERE ItemNumber = @ItemNumber
        AND FacilityCode = @TargetFacilityCode
        AND DispositionId = @DispositionId
        AND IsCommitted = 0
        AND IsActive = 1
    ),
    CommitQty = (
      SELECT ISNULL(SUM(Qty), 0)
      FROM dbo.ItemFacilityDispositionReservationsCommits
      WHERE ItemNumber = @ItemNumber
        AND FacilityCode = @TargetFacilityCode
        AND DispositionId = @DispositionId
        AND IsCommitted = 1
        AND IsActive = 1
    )
WHERE ItemNumber = @ItemNumber
  AND FacilityCode = @TargetFacilityCode
  AND DispositionId = @DispositionId
  AND IsActive = 1;
`
  },
  {
    name: "update-alias-from-join-bare-where",
    sql: `
DECLARE @Facilities dbo.FacilityCodeListType;
DECLARE @ItemNumber VARCHAR(60), @DispositionId INT, @TargetFacilityCode VARCHAR(20);
UPDATE ifda
SET ReservationQty = 1
FROM dbo.ItemFacilityDispositionAvailability ifda
JOIN @Facilities fl ON fl.FacilityCode = ifda.FacilityCode
WHERE ItemNumber = @ItemNumber
  AND FacilityCode = @TargetFacilityCode
  AND DispositionId = @DispositionId
  AND IsActive = 1;
`
  },
  {
    name: "create-proc-shape-with-outputtable",
    sql: `
CREATE PROCEDURE dbo.Repro
  @Facilities dbo.FacilityCodeListType READONLY,
  @ItemNumber VARCHAR(60),
  @DispositionId INT,
  @TargetFacilityCode VARCHAR(20)
AS
BEGIN
  DECLARE @OutputTable TABLE (
    FacilityCode VARCHAR(20),
    AvailableQty INT,
    DispositionCode VARCHAR(50),
    DispositionName VARCHAR(100)
  );

  UPDATE dbo.ItemFacilityDispositionAvailability
  SET ReservationQty = (
        SELECT ISNULL(SUM(Qty), 0)
        FROM dbo.ItemFacilityDispositionReservationsCommits
        WHERE ItemNumber = @ItemNumber
          AND FacilityCode = @TargetFacilityCode
          AND DispositionId = @DispositionId
          AND IsCommitted = 0
          AND IsActive = 1
      )
  WHERE ItemNumber = @ItemNumber
    AND FacilityCode = @TargetFacilityCode
    AND DispositionId = @DispositionId
    AND IsActive = 1;
END;
`
  }
];

let anyRepro = false;
for (const c of cases) {
  const out = runCase(c.name, schemaSql, c.sql);
  if (out.facAmbiguous.length > 0) {
    anyRepro = true;
    console.log(`REPRO FOUND: ${c.name}`);
    for (const d of out.facAmbiguous) {
      console.log(`  - ${d.message} @ line ${d.range.start.line + 1}, col ${d.range.start.character + 1}`);
    }
  } else {
    console.log(`NO REPRO: ${c.name}`);
  }
}

if (!anyRepro) {
  console.log("No FacilityCode ambiguity repro found in current harness cases.");
}
