# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]
### Added

### Changed
- Set working directory of synthetic calculations to the input file dir

### Deprecated

### Removed

### Fixed
- Divide by 0 bug in calculation of synthetic histories
- Adjust recent prices SQL query was improperly including all assets; not only those with dividends or splits in the last 7 days.

### Security

## [0.1.0] - 2022-06-18
### Added
- Calculate adjusted closed for assets with recent splits or dividends
- Calculate synthetic history

[Unreleased]: https://github.com/penny-vault/eod-maintenance/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/penny-vault/eod-maintenance/releases/tag/v0.1.0
