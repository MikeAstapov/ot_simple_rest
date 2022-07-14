
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.13.0] - 2022-07-14
### Added 
- Multiline OTL support
- POST request for check job

## [1.12.0] - 2022-06-16
### Added 
- Backend for user notification of dataset probably trimmed by Spark
- Enhanced functional for uploading SVG files

## [1.11.0] - 2022-05-20
### Added
- Extended logging functional
### Fixed
- Indexes with asterix error message failure

## [1.10.2] - 2022-05-12
### Added
- New error message for index names with asterisk don't match with names in database
- Extra logs in MakeJob
### Fixed
- Fixed the matching of index names with an asterisk with index names in database when performing a search

## [1.10.1] - 2022-04-8
### Added
- Notifications for EVA in checkjob
### Fixed
- Timelines can now work with unordered data
### Changed
- Nginx configuration files which are used in repo replaced
- Pyinstaller version in requirements updated

## [1.10.0] - 2022-02-15
### Added
- Complex datetime string parsing in earliest/latest args including enhanced Splunk relative time modifiers
- Timelines and interesting fields support for S&R

## [1.9.3] - 2021-11-24
### Fixed
- Fix check_cache at makejob that caused makejob fail and immediate run of checkjob even if cache for the job existed

## [1.9.2] - 2021-10-19
### Fixed
- Fix regexep in makejob for using indexes with wildcards (e.g. index=* or index=main*)
- Fix tests

## [1.9.1] - 2021-10-08
### Fixed
- Incorrect NOT statement parsing

## [1.9.0] - 2021-09-09
### Added
- B64 encoding for Scala and Spark code blocks
- Tests for the first point

## [1.8.0] - 2021-07-27
### Added
- Theme endpoints added
- Tests for themes
- SQL migration script

### Changed
- SQL script for eva databases (theme table)

## [1.7.1] - 2021-07-13
### Added
- SSI dispatcher plugin support (b64 decoding)

## [1.6.0] - 2020-09-01
### Added
- Reports
- Endpoint "/api/dashByName"

## [1.5.2] - 2020-08-03
### Fixed
- Installer missing tmp directory 

## [1.5.1] - 2020-07-13
### Fixed
- Broken tests
- List of jobs instead of one raw when checking if one has already been running

## [1.5.0] - 2020-07-10
### Fixed
- Logging in module with job's queue
### Changed
- Jobs with status "NEW" are processed as running ones
- Expired caches can be used if are locked


## [1.4.0] - 2020-06-16
### Added
- Quizs features
- Import export dashboards
- Import export dashboard groups
- User setting endpoint (/api/user/setting), method GET and PUT

## [1.3.3] - 2020-06-16
### Fixed
- Macros dir in Resolver init.
- Remove missed mentions of Splunk and SPL.

## [1.3.2] - 2020-06-05
### Added
- Split script for create and drop DB.
### Fixed
- Fix eva.sql script for add permission on default group.

## [1.3.1] - 2020-06-04
### Fixed
- Connection target db host for tests.

## [1.3.0] - 2020-06-04
### Added
- Macros system.

## [1.2.0] - 2020-06-04
### Changed
- Remove Splunk mentions.
### Added
- All indexes access to admin user.

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
- Fixed using cache\_ttl param from OTL query.

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
