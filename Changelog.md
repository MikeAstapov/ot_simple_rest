
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [1.1.2] - 2020-05-27
### Fixed
- Make test scenario.

## [1.1.1] - 2020-05-27
### Changed
- Deploy strategy to one file binary.

## [1.0.7] - 2020-05-22
### Changed
- Fixed troubles with pool in db connector for jobs handlers.

## [1.0.6] - 2020-05-22
### Changed
- Fixed exists session key issue.

## [1.0.5] - 2020-05-22
### Changed
- Fixed app shutdown exception.

## [1.0.4] - 2020-05-07
### Changed
- Fixed using cache\_ttl param from spl query.

## [1.0.3] - 2020-05-06
### Changed
- Fixed simultainous subsearches running.

## [1.0.2] - 2020-05-06
### Changed
- Fixed filter command in subsearch.
- Updated Docs.

### Added
- Handler & endpoint for SVG loading.
- Tests for EVA handlers.

## [1.0.1] - 2020-04-09
### Changed
- Fixed working of loadjob handler.

## [1.0.0] - 2020-04-06
### Added
- Handler transaction id in logs (for jobs handlers).

### Changed
- Fixed tests for jobs handlers.

## [0.15.4] - 2020-03-25
### Added
- Authorization service.
- Endpoints for EVA application.
- Scheduler for periodic DB manipulations.
- Configuration with NGINX for static content serving.

## [0.14.0] - 2020-02-21
### Added
- Jobs manager for MakeJob jobs synchronization.
- Ability to use NGINX or other for static data serving.
- Work with SQL DB is in a separate module.
- Tests for endpoints.
- Using connection pool for PostgresConnector.

### Fixed
- Simultainously DB inserts in MakeJob handler.

## [0.14.0] - 2020-02-20
### Changed
- Now caches consist not even of dataset, so we add subdir "data" for datasets.  

## [0.13.1] - 2020-02-20
### Fixed
- Command appendpipe quoted masquerading.  

## [0.13.0] - 2020-01-13
### Added
- Command otstats.

### Fixed
- Test fails because of 0.12.8 fixes.

## [0.12.8] - 2019-11-14
### Fixed
- Command search earliest and latest args.  

## [0.12.7] - 2019-11-13
### Fixed
- Strings with indexes instead of array in DB.
- Permissions on wildcard indexes.  

## [0.12.6] - 2019-10-25
###  Changed
- Structure of otrest result cache was changed to identical one of Spark's one for JSONLines.  

### Fixed
- Command otrest now works again.  

## [0.12.5] - 2019-10-25
### Fixed
- Regex pattern for hiding of quoted strings.  

## [0.12.4] - 2019-10-25
### Fixed
- Regex pattern for middle search command.  

## [0.12.3] - 2019-10-23
### Added
- Command oteval capability.  

## [0.12.2] - 2019-10-21
### Fixed
- Now search-command in the middle of SPL will return used fields for pushing down to read command.  

## [0.12.1] - 2019-10-16
### Added
- Changelog.md.

### Changed
- Start using "changelog".

### Fixed
- Special characters like = or - support in read command.
- Remove second pipe before command otinputlookup .

### Removed
- Troubled times because of unknown changes.
