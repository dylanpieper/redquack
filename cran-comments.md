# CRAN Comments

My apologies for the several submissions for this initial release. These changes include:
* Improved parameter names and defaults to align with the REDCap API (e.g., `record_id_name`)
* Fixes for progress tracking messages and audio feedback for an improved user experience
* Improved documentation and use of consistent language

## Addressing previous feedback

1. **References**: 
   * No formal published methods are implemented in this package. The package provides a utility function to connect REDCap and DuckDB, but doesn't implement novel algorithms requiring citation.

2. **LICENSE clarification**:
   * The previous "arrowcap" reference was an older name for the package during development
   * I've corrected the LICENSE files to consistently reference "redquack"

## Changes in this version

* Complete rewrite of database extraction using 'httr2' with retries
* Added automatic resumption of incomplete extractions
* Fixed progress reporting and added a progress bar

## R CMD check results
0 errors | 0 warnings | 0 notes

Thank you for your consideration.
