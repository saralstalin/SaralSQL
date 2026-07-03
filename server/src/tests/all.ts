// Aggregates every test entry point into a single process so coverage tooling
// (node's built-in --experimental-test-coverage) produces one merged report
// instead of needing separate runs stitched together.
import "./validation-layer";
import "./regression-index";
import "./hover-provider";
import "./definition-provider";
import "./references-provider";
import "./completion-provider";
import "./schema-validator";
import "./sql-helpers";
