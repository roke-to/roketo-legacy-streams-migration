const nearAPI = require('near-api-js');
const commandLineArgs = require('command-line-args');
const { execSync } = require('child_process');
const fs = require('fs');
const BigNumber = require('bignumber.js');
const cliProgress = require('cli-progress');
const retry = require('async-retry');
const chalk = require('chalk');

const TICK_TO_S = 1e9;

function getCLIParams() {
  const optionDefinitions = [
    { name: 'accountId', type: String, defaultOption: true },
    { name: 'network', type: String, defaultValue: 'testnet' },
    { name: 'dryRun', type: Boolean, defaultValue: false },
  ];

  return commandLineArgs(optionDefinitions);
}

const options = getCLIParams();

function checkCLIParams(options) {
  if (!options.accountId) {
    console.log(chalk.yellow`Please specify an account ID in [accountId] option whose streams to migrate.`);
    process.exit(1);
  }

  if (options.network !== 'testnet' && options.network !== 'mainnet') {
    console.log(chalk.yellow`Please specify either "mainnet" value for [network] option, or "testnet" (default value).`);
    process.exit(1);
  }
}

const CONFIG = (() => {
  const configs = {
    testnet: {
      roketoContractId: 'streaming-r-v2.dcversus.testnet',
      roketoLegacyContractId: 'dev-1635510732093-17387698050424',
      wrapContractId: 'wrap.testnet',
      nearConfig: {
        networkId: 'testnet',
        nodeUrl: 'https://rpc.testnet.near.org',
        walletUrl: 'https://wallet.testnet.near.org',
      },
    },
    mainnet: {
      roketoContractId: 'streaming.r-v2.near',
      roketoLegacyContractId: 'roketodapp.near',
      wrapContractId: 'wrap.near',
      nearConfig: {
        networkId: 'mainnet',
        nodeUrl: 'https://rpc.mainnet.near.org',
        walletUrl: 'https://wallet.near.org',
      },
    },
  };

  return configs[options.network];
})();

const LEGACY_ROKETO_WITHDRAWAL_FEE_PART = new BigNumber('0.001');
const WITHDRAWN_PART = new BigNumber(1).minus(LEGACY_ROKETO_WITHDRAWAL_FEE_PART);

function getNearInstance() {
  const keyStore = new nearAPI.keyStores.UnencryptedFileSystemKeyStore(
    `${process.env.HOME}/.near-credentials/`
  );

  return nearAPI.connect({
    keyStore,
    ...CONFIG.nearConfig,
  });
}

async function checkAccountIdExistence(accountId, near) {
  try {
    const result = await near.connection.provider.query({
      request_type: 'view_account',
      finality: 'final',
      account_id: accountId,
    });
    return Boolean(result);
  } catch (e) {
    return false;
  }
}

async function getAccount(accountId) {
  const near = await getNearInstance();

  async function getAccountWithFullAccess() {
    const account = await near.account(accountId);

    const keys = await account.findAccessKey();

    if (keys?.accessKey?.permission === 'FullAccess') {
      return account;
    }
  }

  const accountWithFullAccess = await (async () => {
    const existingAccountWithFullAccess = await getAccountWithFullAccess();

    if (existingAccountWithFullAccess) {
      return existingAccountWithFullAccess;
    }

    const exists = await checkAccountIdExistence(accountId, near);

    if (!exists) {
      console.log(chalk.red`Account ID ${accountId} doesn't exist in ${CONFIG.nearConfig.networkId}. Please check that it's specified correctly without typos.`);
      process.exit(1);
    }


    console.log(chalk.cyan`Logging in into ${CONFIG.nearConfig.networkId}...`);
    execSync(`NEAR_ENV=${CONFIG.nearConfig.networkId} yarn near login`);
    console.log(chalk.cyan`Logged in into ${CONFIG.nearConfig.networkId}.`);
    console.log(chalk.cyan`Checking ${accountId} keys...`);

    return getAccountWithFullAccess();
  })();

  if (!accountWithFullAccess) {
    console.log(chalk.red`The script wasn't able to get full access to the specified ${accountId} account. Either try again logging in as the specified account ID, or contact the script developers. Aborting...`);
    process.exit(1);
  }

  console.log(chalk.cyan`Access keys for ${accountId} found. Proceeding...`);
  return accountWithFullAccess;
}

async function getStoragelessAccountIdTickerPairsSet(allActorTickerPairsSet, account) {
  if (allActorTickerPairsSet.size === 0) {
    return allActorTickerPairsSet;
  }

  const bar = new cliProgress.MultiBar({
    stopOnComplete: true,
    forceRedraw: true,
    clearOnComplete: true,
  }, {
    ...cliProgress.Presets.shades_classic,
    format: 'Checking receivers\' FT storage balances:' + cliProgress.Presets.shades_classic.format,
  }).create(allActorTickerPairsSet.size, 0);

  const storagelessAccountIdTickerPairsSet = new Set();

  await Promise.all(Array.from(allActorTickerPairsSet, async (actorTickerPair) => {
    const [actorId, tokenContractId] = actorTickerPair.split('|');

    const tokenContract = new nearAPI.Contract(account, tokenContractId, {
      viewMethods: ['storage_balance_of'],
      changeMethods: [],
    });

    const storage = await tokenContract.storage_balance_of({ account_id: actorId });
    const hasStorageBalance = storage && storage.total !== '0';

    if (!hasStorageBalance) {
      storagelessAccountIdTickerPairsSet.add(actorTickerPair);
    }

    bar.increment();
  }));

  return storagelessAccountIdTickerPairsSet;
}

async function checkIfEnoughNEARs(account, storagelessAccountIdTickerPairsSet, allStreamsCount, nearStreamsCount) {
  const { amount, storage_usage } = await account.state();
  const reservedForTxs = nearAPI.utils.format.parseNearAmount('0.05');

  const availableBalance = new BigNumber(amount).minus(new BigNumber(storage_usage).multipliedBy(1e19)).minus(reservedForTxs);

  const ftStorageRegistrationFeeNear = new BigNumber(storagelessAccountIdTickerPairsSet.size).multipliedBy(nearAPI.utils.format.parseNearAmount('0.0125'));

  const nearStreamsFeeNear = new BigNumber(nearStreamsCount).multipliedBy(nearAPI.utils.format.parseNearAmount('0.1'));

  const streamsStopFeeNear = new BigNumber(allStreamsCount).multipliedBy(nearAPI.utils.format.parseNearAmount('0.001'));
  const withdrawFeeNear = nearAPI.utils.format.parseNearAmount('0.001');
  const operationalFeeNear = streamsStopFeeNear.plus(withdrawFeeNear);

  const reservedForGas = nearAPI.utils.format.parseNearAmount('0.25');

  const totalFeeNear = ftStorageRegistrationFeeNear.plus(nearStreamsFeeNear).plus(streamsStopFeeNear).plus(withdrawFeeNear).plus(reservedForGas);

  console.log(chalk.cyan`[!] Required total available balance: ${nearAPI.utils.format.formatNearAmount(totalFeeNear.toFixed(0), 5)} NEAR (including reserve for gas usage).`);

  if (ftStorageRegistrationFeeNear.isGreaterThan(0)) {
    console.log(chalk.cyan`[!] Up to ${nearAPI.utils.format.formatNearAmount(ftStorageRegistrationFeeNear.toFixed(0), 5)} NEAR for covering FT storage registrations.`);
  }

  console.log(chalk.cyan`[!] ${nearAPI.utils.format.formatNearAmount(operationalFeeNear.toFixed(0), 5)} NEAR for covering operational costs for stopping streams and withdrawal.`);

  if (nearStreamsFeeNear.isGreaterThan(0)) {
    console.log(chalk.cyan`[!] ${nearAPI.utils.format.formatNearAmount(nearStreamsFeeNear.toFixed(0), 5)} NEAR for Roketo stream creation fees.`);
  }

  console.log(chalk.cyan`\n[!] Current available balance: ${nearAPI.utils.format.formatNearAmount(availableBalance.toFixed(0), 5)} NEAR.`);

  if (totalFeeNear.isGreaterThan(availableBalance)) {
    console.log(chalk.red([
      `Not enough NEARs on ${account.accountId} account for proceeding.`,
      `Please add ${nearAPI.utils.format.formatNearAmount(totalFeeNear.minus(availableBalance).toFixed(0), 5)} more NEAR to the account before proceeding.`,
      `Aborting...`,
    ].join('\n')));

    process.exit(1);
  }

  console.log(chalk.green`?????? There're enough NEARs on ${account.accountId} for proceeding.`);
}

async function stopLegacyStreams(account, outgoingLegacyStreams, cacheFilename) {
  if (outgoingLegacyStreams.length === 0) {
    return;
  }

  console.log(chalk.cyan`Stopping legacy streams...`);

  const bar = new cliProgress.MultiBar({
    stopOnComplete: true,
    forceRedraw: true,
    clearOnComplete: true,
  }, {
    ...cliProgress.Presets.shades_classic,
    format: 'Stopping legacy streams:' + cliProgress.Presets.shades_classic.format,
  }).create(outgoingLegacyStreams.length, 0);

  let failedStreamsCount = 0;

  await Promise.all(outgoingLegacyStreams.map(async (outgoingLegacyStream) => {
    const action = nearAPI.transactions.functionCall(
      'stop_stream',
      {
        stream_id: outgoingLegacyStream.id,
      },
      '100000000000000',
      '1000000000000000000000',
    );

    await retry(
      async () => {
        try {
          if (outgoingLegacyStream.status === 'ACTIVE') {
            const cache = (() => {
              try {
                const cacheString = fs.readFileSync(cacheFilename, { encoding: 'utf-8' })
                return JSON.parse(cacheString);
              } catch {
                return {};
              }
            })();
            if (!cache[outgoingLegacyStream.id]?.stream) {
              cache[outgoingLegacyStream.id] = {};
            }
            cache[outgoingLegacyStream.id].stream = outgoingLegacyStream;
            fs.writeFileSync(cacheFilename, JSON.stringify(cache, null, 2));
          }

          const finalExecutionOutcome = await account.signAndSendTransaction({
            receiverId: CONFIG.roketoLegacyContractId,
            actions: [action],
          });

          const hasFailed = finalExecutionOutcome.receipts_outcome.some(
            (receipt) => receipt.outcome.status === 'Failure' || 'Failure' in receipt.outcome.status
          );

          if (hasFailed) {
            throw new Error('Failed to stop legacy stream');
          }

          bar.increment();
        } catch (err) {
          if (
            err.message === 'Transaction has expired' ||
            err.message.includes(`GatewayTimeoutError`) ||
            err.message.includes(`Please try again`)
          ) {
            throw new Error('Try again');
          } else {
            console.log(chalk.red`signAndSignTransaction error`);
            console.error(err);

            failedStreamsCount += 1;

            bar.increment();
          }
        }
      },
      {
        retries: 10,
        minTimeout: 500,
        maxTimeout: 1500,
      }
    );
  }));

  if (failedStreamsCount > 0) {
    console.log(chalk.red`The script failed to stop ${failedStreamsCount}/${outgoingLegacyStreams.length} legacy streams.`);
    console.log(chalk.red`Please try running the script with the same parameters again, continuing from the previous state.`);
    console.log(chalk.red`If the error persists, contact developers from README.md.`);
    process.exit(1);
  } else {
    console.log(chalk.green`?????? All legacy streams were stopped.`);
  }
}

async function createStorageDeposits(account, accountIdTickerPairsSet) {
  if (accountIdTickerPairsSet.size === 0) {
    return;
  }

  console.log(chalk.cyan`Registering actors legacy streams...`);

  const bar = new cliProgress.MultiBar({
    stopOnComplete: true,
    forceRedraw: true,
    clearOnComplete: true,
  }, {
    ...cliProgress.Presets.shades_classic,
    format: 'Creating FT storages:' + cliProgress.Presets.shades_classic.format,
  }).create(accountIdTickerPairsSet.size, 0);

  await Promise.all(Array.from(accountIdTickerPairsSet).map(async (accountIdTickerPair) => {
    const [actorId, tokenContractId] = accountIdTickerPair.split('|');
    const depositAmount = nearAPI.utils.format.parseNearAmount('0.0125'); // account creation costs up to 0.0125 NEAR for storage

    const actions = [nearAPI.transactions.functionCall(
      'storage_deposit',
      {
        account_id: actorId,
        registration_only: true,
      },
      '30000000000000',
      depositAmount,
    )];

    await retry(
      async () => {
        try {
          await account.signAndSendTransaction({
            receiverId: tokenContractId,
            actions,
          });

          bar.increment();
        } catch (err) {
          if (
            err.message === 'Transaction has expired' ||
            err.message.includes(`GatewayTimeoutError`) ||
            err.message.includes(`Please try again`)
          ) {
            throw new Error('Try again');
          } else {
            console.log(chalk.red`signAndSignTransaction error`);
            console.error(err);
            console.log(chalk.red`Please try running the script with the same parameters again, continuing from the previous state.`);
            console.log(chalk.red`If the error persists, contact developers from README.md.`);
            process.exit(1);
          }
        }
      },
      {
        retries: 10,
        minTimeout: 500,
        maxTimeout: 1500,
      }
    );
  }));

  console.log(chalk.green`?????? All needed FT storages were created.`);
}

async function withdrawAll(account) {
  console.log(chalk.cyan`Withdrawing all the valuables...`);

  await retry(
    async () => {
      try {
        await account.signAndSendTransaction({
          receiverId: CONFIG.roketoLegacyContractId,
          actions: [
            nearAPI.transactions.functionCall(
              'update_account',
              { account_id: account.accountId },
              '200000000000000',
              nearAPI.utils.format.parseNearAmount('0.001'),
            )
          ],
        });
      } catch (err) {
        if (
          err.message === 'Transaction has expired' ||
          err.message.includes(`GatewayTimeoutError`) ||
          err.message.includes(`Please try again`)
        ) {
          throw new Error('Try again');
        } else {
          console.log(chalk.red`signAndSignTransaction error`);
          console.error(err);
        }
      }
    },
    {
      retries: 10,
      minTimeout: 500,
      maxTimeout: 1500,
    }
  );

  console.log(chalk.green`?????? Withdrawn all the valuables.`);
}

async function createStreams(account, cacheFilename, tickersToContractIdsMap) {
  const cache = (() => {
    try {
      const cacheString = fs.readFileSync(cacheFilename, { encoding: 'utf-8' })
      return JSON.parse(cacheString);
    } catch {
      return {};
    }
  })();

  const totalToCreate = Object.keys(cache).length;

  if (totalToCreate === 0) {
    console.log(chalk.green`No streams to recreate.`);
    return;
  }

  const bar = new cliProgress.MultiBar({
    stopOnComplete: true,
    forceRedraw: true,
    clearOnComplete: true,
  }, {
    ...cliProgress.Presets.shades_classic,
    format: 'Creating streams:' + cliProgress.Presets.shades_classic.format,
  }).create(totalToCreate, 0);

  let failedStreamsCount = 0;

  await Promise.all(Object.values(cache).map(async ({ stream, finalTokensWithdrawn }) => {
    const amountInYocto = new BigNumber(stream.balance).plus(stream.tokens_total_withdrawn).minus(finalTokensWithdrawn).multipliedBy(WITHDRAWN_PART);

    const comment = stream.description;
    const tokensPerSec = new BigNumber(stream.tokens_per_tick).multipliedBy(TICK_TO_S).toFixed(0);

    const nearStreamCreationFee = nearAPI.utils.format.parseNearAmount('0.1');

    const actions = [
      nearAPI.transactions.functionCall(
        'ft_transfer_call',
        {
          receiver_id: CONFIG.roketoContractId,
          amount: stream.ticker === 'NEAR'
            ? amountInYocto.plus(nearStreamCreationFee).toFixed(0)
            : amountInYocto.toFixed(0),
          memo: 'Roketo transfer',
          msg: JSON.stringify({
            Create: {
              request: {
                owner_id: account.accountId,
                receiver_id: stream.receiver_id,
                balance: amountInYocto.toFixed(0),
                tokens_per_sec: tokensPerSec,
                is_auto_start_enabled: true,
                ...comment && {
                  description: JSON.stringify({
                    c: comment.length > 80 ? `${comment.slice(0, 77)}...` : comment,
                  }),
                },
              },
            },
          }),
        },
        '100000000000000',
        '1',
      )
    ];

    if (stream.ticker === 'NEAR') {
      actions.unshift(
        nearAPI.transactions.functionCall(
          'near_deposit',
          {},
          '30000000000000',
          amountInYocto.plus(nearStreamCreationFee).toFixed(0),
        ),
      );
    }

    await retry(
      async () => {
        try {
          const finalExecutionOutcome = await account.signAndSendTransaction({
            receiverId: tickersToContractIdsMap[stream.ticker],
            actions,
          });

          const hasFailed = finalExecutionOutcome.receipts_outcome.some(
            (receipt) => receipt.outcome.status === 'Failure' || 'Failure' in receipt.outcome.status
          );

          if (hasFailed) {
            throw new Error('Failed to create stream');
          }

          bar.increment();
        } catch (err) {
          if (
            err.message === 'Transaction has expired' ||
            err.message.includes(`GatewayTimeoutError`) ||
            err.message.includes(`Please try again`)
          ) {
            throw new Error('Try again');
          } else {
            console.log(chalk.red`signAndSignTransaction error`);
            console.error(err);

            failedStreamsCount += 1;

            bar.increment();
          }
        }
      },
      {
        retries: 10,
        minTimeout: 500,
        maxTimeout: 1500,
      }
    );
  }));

  if (failedStreamsCount > 0) {
    console.log(chalk.red`The script failed to create ${failedStreamsCount}/${totalToCreate} streams.`);
    console.log(chalk.red`Please try running the script with the same parameters again, continuing from the previous state.`);
    console.log(chalk.red`If the error persists, contact developers from README.md.`);
  } else {
    console.log(chalk.green`?????? All streams were created.`);
  }
}

async function fillFinalWithdrawns(cacheFilename, legacyRoketoContract) {
  const cache = (() => {
    try {
      const cacheString = fs.readFileSync(cacheFilename, { encoding: 'utf-8' })
      return JSON.parse(cacheString);
    } catch {
      return {};
    }
  })();

  const streams = await Promise.all(Object.values(cache).filter(({ finalTokensWithdrawn }) => !finalTokensWithdrawn).map(({ stream }) => legacyRoketoContract.get_stream({ stream_id: stream.id })));
  streams.forEach(({ balance, id, tokens_total_withdrawn }) => {
    if (balance !== '0') {
      console.log(chalk.red`Stream with ID ${id} is supposed to be stopped but doesn't have zero balance. Please contact script developers. Aborting...`);
      process.exit(1);
    }

    const cache = (() => {
      try {
        const cacheString = fs.readFileSync(cacheFilename, { encoding: 'utf-8' })
        return JSON.parse(cacheString);
      } catch {
        return {};
      }
    })();
    if (new BigNumber(cache[id].stream.balance).plus(cache[id].stream.tokens_total_withdrawn).minus(tokens_total_withdrawn).isZero()) {
      delete cache[id];
    } else {
      cache[id].finalTokensWithdrawn = tokens_total_withdrawn;
    }
    fs.writeFileSync(cacheFilename, JSON.stringify(cache, null, 2));
  });
}

const main = async () => {
  checkCLIParams(options);

  const account = await getAccount(options.accountId);
  const cacheFilename = `${account.accountId}.cache.json`;

  const legacyRoketoContract = new nearAPI.Contract(account, CONFIG.roketoLegacyContractId, {
    viewMethods: ['get_account', 'get_stream', 'get_status'],
    changeMethods: ['stop_stream'],
  });

  console.log(chalk.cyan`Fetching account info...`);
  const { dynamic_inputs, dynamic_outputs, static_streams } = await (async () => {
    try {
      const roketoAccount = await legacyRoketoContract.get_account({ account_id: account.accountId });
      if (roketoAccount) {
        return roketoAccount;
      }
    } catch {/* NO-OP */}

    console.log(chalk.green`${account.accountId} hasn't used legacy Roketo contract. Exiting...`);
    process.exit(0);
  })();

  const nonRecreatedStreamsCache = (() => {
    try {
      const streamsMapCacheString = fs.readFileSync(cacheFilename, { encoding: 'utf-8' });

      return JSON.parse(streamsMapCacheString);
    } catch {
      return {};
    }
  })();

  const streamsReceiverSpeedBalancesMap = Object.values(nonRecreatedStreamsCache).reduce(
    (acc, { stream, finalTokensWithdrawn }) => finalTokensWithdrawn
      ? Object.assign(acc, {
        [`${
          stream.receiver_id
        }|${
          new BigNumber(stream.tokens_per_tick).multipliedBy(TICK_TO_S).toFixed(0)
        }|${
          new BigNumber(stream.balance).plus(stream.tokens_total_withdrawn).minus(finalTokensWithdrawn).multipliedBy(WITHDRAWN_PART).toFixed(0)
        }`]: stream.id,
      })
      : acc,
    {}
  );

  const { roketoContractId } = CONFIG;
  const roketoContract = new nearAPI.Contract(account, roketoContractId, {
    viewMethods: ['get_account_outgoing_streams'],
    changeMethods: [],
  });

  const streams = await (async () => {
    try {
      return await roketoContract.get_account_outgoing_streams({ account_id: account.accountId, from: 0, limit: 99999 });
    } catch {
      console.log(chalk.red`Failed to get streams of ${account.accountId}. Please contact script developers. Aborting...`);
      process.exit(1);
    }
  })();

  streams.forEach(({ receiver_id, tokens_per_sec, balance, tokens_total_withdrawn }) => {
    const key = `${
      receiver_id
    }|${
      tokens_per_sec
    }|${
      new BigNumber(balance).plus(tokens_total_withdrawn).toFixed(0)
    }`;

    if (key in streamsReceiverSpeedBalancesMap) {
      const cache = (() => {
        try {
          const cacheString = fs.readFileSync(cacheFilename, { encoding: 'utf-8' })
          return JSON.parse(cacheString);
        } catch {
          return {};
        }
      })();
      delete cache[streamsReceiverSpeedBalancesMap[key]];
      fs.writeFileSync(cacheFilename, JSON.stringify(cache, null, 2));

      delete nonRecreatedStreamsCache[streamsReceiverSpeedBalancesMap[key]];
    }
  });

  const nonRecreatedStreams = Object.values(nonRecreatedStreamsCache).map(({ stream }) => stream);
  const nonRecreatedStreamsLength = Object.keys(nonRecreatedStreamsCache).length;

  const legacyStreamsIds = Array.from(new Set([...dynamic_inputs, ...dynamic_outputs, ...static_streams]));

  console.log(chalk.cyan`Fetching legacy streams info...`);
  const legacyStreams = await Promise.all(legacyStreamsIds.map((legacyStreamsId) => legacyRoketoContract.get_stream({ stream_id: legacyStreamsId })));

  const outgoingLegacyStreams = legacyStreams.filter(Boolean).filter(({ owner_id, status }) =>
    owner_id === account.accountId && (status === 'ACTIVE' || status === 'PAUSED' || status === 'INITIALIZED')
  );

  const activeStreams = outgoingLegacyStreams.filter(({ status }) => status === 'ACTIVE');
  const pausedStreams = outgoingLegacyStreams.filter(({ status }) => status === 'PAUSED');
  const initializedStreams = outgoingLegacyStreams.filter(({ status }) => status === 'INITIALIZED');

  const nonActiveStreamsCount = pausedStreams.length + initializedStreams.length;

  const totalStreamsCount = activeStreams.length + nonActiveStreamsCount;

  if (totalStreamsCount === 0 && nonRecreatedStreamsLength === 0) {
    console.log(chalk.green`No non-finished legacy streams found. Exiting...`);
    if (fs.existsSync(cacheFilename)) {
      fs.unlinkSync(cacheFilename);
    }
    process.exit(0);
  }

  console.log();
  if (totalStreamsCount > 0) {
    console.log(chalk.magenta`[!] Non-finished legacy streams found: ${totalStreamsCount}.`);
    console.log(chalk.magenta`[!] The script will stop all of them.`);
    if (nonActiveStreamsCount === 0) {
      console.log(chalk.magenta`[!] All of them will be recreated.`);
    } else if (activeStreams.length > 0) {
      console.log(chalk.magenta`[!] ${activeStreams.length} of them will be recreated (without ${nonActiveStreamsCount} paused/initialized).`);
    }
  }
  if (nonRecreatedStreamsLength > 0) {
    console.log(chalk.magenta`[!] ${nonRecreatedStreamsLength} streams from previous runs will be created.`);
  }
  console.log();

  const legacyRoketoContractStatus = await legacyRoketoContract.get_status();

  const tickersToContractIdsMap = legacyRoketoContractStatus.tokens.reduce(
    (map, { ticker, account_id }) => Object.assign(map, { [ticker]: ticker === 'NEAR' ? CONFIG.wrapContractId : account_id }),
    {}
  );

  const allActorTickerPairsSet = new Set(
    activeStreams.flatMap(
      ({ owner_id, receiver_id, ticker }) => [
        `${owner_id}|${tickersToContractIdsMap[ticker]}`,
        `${receiver_id}|${tickersToContractIdsMap[ticker]}`,
      ]
    )
  );

  const storagelessAccountIdTickerPairsSet = await getStoragelessAccountIdTickerPairsSet(allActorTickerPairsSet, account);

  const allStreamsCount = outgoingLegacyStreams.length;
  const nearStreamsCount = [...outgoingLegacyStreams, ...nonRecreatedStreams].filter(({ ticker }) => ticker === 'NEAR').length;

  await checkIfEnoughNEARs(account, storagelessAccountIdTickerPairsSet, allStreamsCount, nearStreamsCount);

  if (options.dryRun) {
    console.log(chalk.yellow`[dryRun] option specified. Omit this option to actually recreate all the streams. Exiting...`);
    process.exit(0);
  }

  await createStorageDeposits(account, storagelessAccountIdTickerPairsSet);

  await stopLegacyStreams(account, outgoingLegacyStreams, cacheFilename);

  await fillFinalWithdrawns(cacheFilename, legacyRoketoContract);

  const needsWithdrawal = await (async () => {
    const roketoAccount = await legacyRoketoContract.get_account({ account_id: account.accountId });

    return roketoAccount.ready_to_withdraw.some(([, sum]) => sum !== '0');
  })();

  if (needsWithdrawal) {
    await withdrawAll(account, legacyRoketoContract);
  }

  await createStreams(account, cacheFilename, tickersToContractIdsMap);

  try {
    const cacheString = fs.readFileSync(cacheFilename, { encoding: 'utf-8' })
    const cache = JSON.parse(cacheString);
    if (Object.keys(cache).length === 0) {
      fs.unlinkSync(cacheFilename);
    }
  } catch {}
};

main();
