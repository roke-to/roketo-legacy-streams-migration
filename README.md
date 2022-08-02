# Roke.to legacy streams migration script

## Usage guide

1. Install `node` version `>=16` and `yarn` version `1`.
2. Install node packages:

```bash
yarn
```

3. Run the script specifying an `accountId`:

```bash
yarn migrate-roketo-legacy-streams --network=mainnet [accountId]
```

## Required options

* `accountId`: account id to migrate scripts from.

## Optional options
* `network`: `mainnet` or `testnet` (default);
* `dryRun`: specify this flag for a dry run without any actual actions.

## Cache

The script creates a cache file ending with `.cache.json` near the script for caching purposes. After all legacy streams for an account are migrated, the cache file is deleted.

## Example

E.g. if `lebedev.testnet` is to dry run Roketo legacy streams migration, the command would be:

```bash
yarn migrate-roketo-legacy-streams --dryRun lebedev.testnet
```

## Support

Tested on Ubuntu Linux.

If you experience any issued, you can write to https://t.me/angly or https://t.me/dcversus for support.