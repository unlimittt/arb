import { Injectable, Logger, OnModuleInit } from '@nestjs/common'
import { InjectModel } from '@nestjs/sequelize'
import {
  FlashbotsBundleProvider,
  FlashbotsBundleResolution,
  FlashbotsTransactionResponse,
  RelayResponseError,
} from '@flashbots/ethers-provider-bundle'
import chalk from 'chalk'
import { BigNumber, providers, utils } from 'ethers'
import PQueue from 'p-queue'
import { DataService } from '../data/data.service'
import { CalculatorService } from '../calculator/calculator.service'
import { Dict, Path, Track } from '../types'
import { LogModel } from '../log/log.model'
import { Chain, chain, LogType, PathType, Protocol, ProtocolGroup } from '../constants'
import BasePool from '../pool/variant/BasePool'
import { EthChainService, Method } from '../chain/eth.chain.service'
import * as comlink from 'comlink'
import SimWorker from './sim.worker'
import { TransactionRequest } from '@ethersproject/abstract-provider'
import { uniq } from 'underscore'
import config from '../config'
import EthPathBundle from '../path/variant/EthPathBundle'
import EthPath from '../path/variant/EthPath'
import { PathModel } from '../path/path.model'
import GasWorker from '../gas/gas.worker'
import path from 'path'
import { Worker } from 'worker_threads'
import nodeEndpoint from 'comlink/dist/umd/node-adapter'
import { PriceService } from '../price/price.service'
import { BalanceService } from '../balance/balance.service'
import OneInchLimitOrderPool from '../pool/variant/OneInchLimitOrderPool'

@Injectable()
export class EthSwapService implements OnModuleInit {
  private readonly logger = new Logger(EthSwapService.name)
  private tracking: Track[] = []
  private baseFeeB: BigNumber
  private maxNextBaseFee: BigNumber
  private gasFee: number
  private readonly queue: PQueue
  private provider: providers.Provider
  private lastTargetBlock: number
  private readonly simWorkers: { instance: comlink.Remote<SimWorker>; busy: boolean }[]
  private readonly gasWorkers: { instance: comlink.Remote<GasWorker>; busy: boolean }[]
  private readonly simWorkerByTxHash: Dict<comlink.Remote<SimWorker>>
  private doUnwrap: boolean

  constructor(
    private readonly chainService: EthChainService,
    private readonly dataService: DataService,
    private readonly calculatorService: CalculatorService,
    private readonly priceService: PriceService,
    private readonly balanceService: BalanceService,
    @InjectModel(LogModel) private logModel: typeof LogModel,
    @InjectModel(PathModel) private pathModel: typeof PathModel
  ) {
    this.queue = new PQueue({ concurrency: 1 })
    this.simWorkers = []
    this.gasWorkers = []
    this.simWorkerByTxHash = {}
    this.doUnwrap = config.unwrap
  }

  async onModuleInit() {
    if (chain === Chain.ETH) {
      this.provider = this.chainService.providers.default
      // await this.dataService.getPoolsFromChain(this.provider)
      // await this.dataService.buildPaths()
      // await this.dataService.gasEstimate(this.chainService, this.calculatorService)
      this.chainService.lastBlock = await this.dataService.loadData(this.provider, this.calculatorService)
      await this.priceService.init()
      this.balanceService.init()
      await this.setupBlockChain()
    }
  }

  async setupBlockChain() {
    this.baseFeeB = await this.chainService.getBaseFee()

    this.provider.on('block', async blockNumber => {
      await this.queue.add(async () => {
        await this.onBlock(blockNumber)
      })
    })

    // if (config.sim) {
    //   this.provider.on('pending', async txHash => {
    //     await this.onPending(txHash)
    //   })
    //
    //   for (let i = 0; i < config.simWorkers; i++) {
    //     const filename = path.join(__dirname, './sim.worker.js')
    //     const worker = new Worker(filename)
    //     this.simWorkers.push({ instance: comlink.wrap<SimWorker>(nodeEndpoint(worker)), busy: false })
    //   }
    // }

    for (let i = 0; i < config.gasWorkers; i++) {
      const filename = path.join(__dirname, '../gas/gas.worker.js')
      const worker = new Worker(filename)
      this.gasWorkers.push({ instance: comlink.wrap<GasWorker>(nodeEndpoint(worker)), busy: false })
    }

    // await this.checkPools()
    setInterval(this.checkPools.bind(this), 300000) // 5 min
  }

  async onBlock(blockNumber: number) {
    if (blockNumber < this.chainService.lastBlock + 1) return

    this.baseFeeB = await this.chainService.getBaseFee(blockNumber)
    this.maxNextBaseFee = FlashbotsBundleProvider.getMaxBaseFeeInFutureBlock(this.baseFeeB, 1)
    this.gasFee = parseFloat(utils.formatEther(this.baseFeeB))

    this.logger.log(
      [
        `Block: ${blockNumber}`,
        `BaseFee: ${utils.formatUnits(this.baseFeeB, 'gwei')}`,
        `MaxNextBaseFee: ${utils.formatUnits(this.maxNextBaseFee, 'gwei')}`,
      ].join(', ')
    )

    if (this.doUnwrap) {
      this.doCheckBalance(blockNumber)
    }

    const logs = await this.chainService.getLogs(blockNumber)
    const [affectedPools, burningTokenPools] = await this.chainService.handleLogs({
      data: this.dataService.data,
      logs,
    })
    this.chainService.lastBlock = blockNumber
    const trackingPools = this.tracking.map(track => track.pool)
    trackingPools.push(this.dataService.data.poolById[11706])
    await Promise.all(burningTokenPools.map(pool => pool.loadData(blockNumber)))
    const pools: BasePool[] = uniq([].concat(affectedPools, trackingPools, burningTokenPools))

    const profitablePaths = await this.calculatorService.findProfitablePaths(pools, this.gasFee)
    // profitablePaths.forEach(path => {
    //   if ([723399].includes(path.id)) {
    //     path.optimalAmount = 0.779
    //     path.profit = 0.012
    //   }
    // })
    this.updateTrackingPools(profitablePaths)
    await this.swap(profitablePaths)
  }

  updateTrackingPools(profitablePaths: Path[]) {
    const oldTracks = this.tracking
    this.tracking = []
    for (const profitablePath of profitablePaths) {
      for (const pool of profitablePath.pools) {
        let track = { pool, count: 1 }
        for (const oldTrack of oldTracks) {
          if (oldTrack.pool === pool) {
            oldTrack.count++
            track = oldTrack
          }
        }
        this.tracking.push(track)
      }
    }
  }

  async onPending(txHash: string) {
    //   const worker = this.simWorkers.find(worker => !worker.busy)
    //   if (worker) {
    //     worker.busy = true
    //     const { logs, gasPrice } = await worker.instance.simulate(txHash)
    //     worker.busy = false
    //
    //     if (logs) {
    //       const sim = true
    //
    //       this.simWorkerByTxHash[txHash] = worker.instance
    //       setTimeout(() => {
    //         delete this.simWorkerByTxHash[txHash]
    //       }, 60000)
    //
    //       await this.queue.add(async () => {
    //         const [affectedPools] = await this.chainService.handleLogs({
    //           data: this.dataService.data,
    //           logs,
    //           sim,
    //         })
    //         const profitablePaths = await this.calculatorService.findProfitablePaths(affectedPools, this.gasFee)
    //         await this.swap(profitablePaths, txHash)
    //         await Promise.all(affectedPools.map(pool => pool.setSim(false)))
    //       })
    //     }
    //   }
  }

  async checkPools() {
    const incorrectPools: BasePool[] = []
    const { pools } = this.dataService.data

    for (const pool of pools) {
      // if (pool.id === 11689) {
      //   incorrectPools.push(pool)
      // }
      await this.queue.add(async () => {
        try {
          const isCorrect = await pool.verifyData(this.chainService.lastBlock)
          if (!isCorrect) incorrectPools.push(pool)
        } catch (e) {
          pool.logger.error(e?.error || e?.message)
        }
      })
    }

    // if (Object.keys(this.dataService.data.poolByOrderHash).length > 0) {
    //   await this.queue.add(async () => {
    //     await OneInchLimitOrderPool.verifyData(this.dataService.data)
    //   })
    // }

    await this.queue.add(async () => {
      const profitablePaths = await this.calculatorService.findProfitablePaths(incorrectPools, this.gasFee)
      await this.swap(profitablePaths)
    })
  }

  async doCheckBalance(blockNumber: number) {
    const balance = parseFloat(utils.formatEther(await this.chainService.wallet.getBalance(blockNumber)))

    if (balance < 0.04) {
      this.logger.log(`Balance: ${balance}, unwrapping...`)
      this.doUnwrap = false

      const { weth, swap, swap2 } = this.chainService.contracts
      const contract1Balance: BigNumber = await weth.balanceOf(swap.address)
      const contract2Balance: BigNumber = await weth.balanceOf(swap2.address)

      await (contract1Balance.gt(contract2Balance)
        ? this.unwrap(contract1Balance, swap.address)
        : this.unwrap(contract2Balance, swap2.address))

      setTimeout(() => {
        this.doUnwrap = true
      }, 3600000) // 1 hour
    }
  }

  async unwrap(balance: BigNumber, to: string) {
    const wallet = this.chainService.wallet
    const amount = balance.sub(utils.parseEther('0.1'))

    await wallet.sendTransaction({
      to,
      nonce: this.chainService.getNonce(wallet),
      data: utils.hexConcat([Method.UNWRAP, utils.hexZeroPad(amount.toHexString(), 14)]),
      type: 2,
      maxFeePerGas: utils.parseUnits('150', 'gwei'),
      maxPriorityFeePerGas: utils.parseUnits('1', 'gwei'),
      gasLimit: 55000,
    })
  }

  async estimateGas(
    blockNumber: number,
    path: EthPathBundle | EthPath,
    transaction: TransactionRequest,
    pendingTxHash?: string
  ): Promise<number> {
    let gas = 0
    try {
      if (pendingTxHash) {
        const worker = this.simWorkerByTxHash[pendingTxHash]
        if (worker) {
          const t = { ...transaction }
          if (t.gasPrice) t.gasPrice = t.gasPrice.toString()
          if (t.maxFeePerGas) t.maxFeePerGas = t.maxFeePerGas.toString()
          if (t.maxPriorityFeePerGas) t.maxPriorityFeePerGas = t.maxPriorityFeePerGas.toString()
          gas = await worker.estimateGas(t, pendingTxHash)
        }
      } else {
        // send to free gasWorker
        const worker = this.gasWorkers.find(worker => !worker.busy)
        if (worker) {
          worker.busy = true
          gas = await worker.instance.estimateGas(transaction, blockNumber)
          worker.busy = false
        }
      }
    } catch (e) {
      const message = e.error?.toString() || e.message?.toString()
      path.logger.error(message)
      const isCorrect = await path.verifyData(blockNumber)

      await this.logModel.create({
        blockNumber: blockNumber,
        pathId: path instanceof EthPath ? path.id : path.paths[0].id,
        type: LogType.ESTIMATE_GAS,
        message: message.substring(0, 255),
        swapParams: transaction.data,
        other: {
          isCorrect,
          to: transaction.to,
          paths: path instanceof EthPath ? [path.id] : path.paths.map(path => path.id),
        },
      })
    }

    if (path instanceof EthPath) {
      path.estimatedGas = gas > 120000 ? gas : 0
    }

    // if (!gas) {
    //   console.log(transaction)
    //   if (path instanceof EthPath) {
    //     console.log((path as EthPath).params)
    //   } else {
    //     ;(path as EthPathBundle).paths.forEach(path => console.log(path.params))
    //   }
    // }

    return gas
  }

  async swap(profitablePaths: Path[], pendingTxHash: string = null) {
    if (profitablePaths.length === 0) return

    const sim = !!pendingTxHash
    const flashbots = <FlashbotsBundleProvider>this.chainService.providers.flashbots
    const wallet = this.chainService.wallet
    const contract1 = this.chainService.contracts.swap
    const contract2 = this.chainService.contracts.swap2
    const balance1 = parseFloat(utils.formatEther(await this.chainService.contracts.weth.balanceOf(contract1.address)))
    const balance2 = parseFloat(utils.formatEther(await this.chainService.contracts.weth.balanceOf(contract2.address)))
    const baseFee = parseFloat(utils.formatEther(this.baseFeeB))
    const lastBlock = this.chainService.lastBlock
    const targetBlock = lastBlock + 1
    const walletBalance = parseFloat(utils.formatEther(await wallet.getBalance(lastBlock)))
    const nonce = await this.chainService.getNonce(wallet)
    const pathBundle = new EthPathBundle(profitablePaths.map(path => new EthPath(path)))

    const transaction: TransactionRequest = {
      from: wallet.address,
      chainId: 1,
      nonce,
      type: 2,
      maxFeePerGas: this.maxNextBaseFee,
      maxPriorityFeePerGas: this.maxNextBaseFee,
      gasLimit: 2500000,
    }

    // logging
    pathBundle.paths.forEach(path => {
      path.logger.verbose(
        [
          `Amount:${path.optimalAmount.toFixed(8)}`,
          `Profit:${path.profit.toFixed(8)}`,
          `ProfitNet:${path.profitNet.toFixed(8)}`,
        ].join(', ')
      )
    })

    // params
    await pathBundle.setSwapParams(lastBlock, this.simWorkerByTxHash[pendingTxHash], pendingTxHash)

    pathBundle.paths = pathBundle.paths
      // affordable
      .filter(path => {
        const isLastPoolUniswapV3Pool = path.pools[path.pools.length - 1].protocolGroup === ProtocolGroup.UNISWAP_V3
        const isLastPoolUniswapV2Pool = path.pools[path.pools.length - 1].protocolGroup === ProtocolGroup.UNISWAP_V2
        return path.type === PathType.ALL
          ? balance2 > path.optimalAmount || isLastPoolUniswapV3Pool || isLastPoolUniswapV2Pool
          : balance1 > path.optimalAmount || isLastPoolUniswapV3Pool
      })
      // profit calculated ok
      .filter(path => {
        const previousProfit = path.profit
        const amountOut = parseFloat(utils.formatEther(path.params.amounts[path.params.amounts.length - 1]))
        path.profit = amountOut - path.optimalAmount
        if (previousProfit > path.profit * 1.05) {
          path.logger.warn(`Incorrect profit: ${previousProfit} -> ${path.profit}`)
          Promise.all(path.pools.map(pool => pool.verifyData(lastBlock)))
          return false
        }
        return true
      })

    // estimateGas each path individually for correct execution
    console.time('EstimateGas')
    await Promise.all(
      pathBundle.paths.map(async path => {
        await this.estimateGas(
          lastBlock,
          path,
          {
            ...transaction,
            to: path.type === PathType.ALL ? contract2.address : contract1.address,
            data: path.toData(
              targetBlock,
              path.type === PathType.ALL &&
                path.pools[path.pools.length - 1].protocolGroup === ProtocolGroup.UNISWAP_V2 &&
                balance2 < path.optimalAmount
            ),
          },
          pendingTxHash
        )
      })
    )
    console.timeEnd('EstimateGas')

    // filter
    pathBundle.paths = pathBundle.paths
      // estimated ok
      .filter(path => path.estimatedGas)
      // profitable
      .filter(path => (baseFee * path.estimatedGas) / path.profit < 1)

    const currentBlock = await this.chainService.getBlockNumber()
    const isMissed = currentBlock > lastBlock

    // filter affordable altogether
    pathBundle.setPathType()
    pathBundle.paths = pathBundle.paths.filter(path => {
      const isLastPoolUniswapV3Pool = path.pools[path.pools.length - 1].protocolGroup === ProtocolGroup.UNISWAP_V3
      const isLastPoolUniswapV2Pool = path.pools[path.pools.length - 1].protocolGroup === ProtocolGroup.UNISWAP_V2
      return (
        (pathBundle.type === PathType.ALL ? balance2 : balance1) > path.optimalAmount ||
        isLastPoolUniswapV3Pool ||
        (pathBundle.paths.length === 1 && pathBundle.type === PathType.ALL && isLastPoolUniswapV2Pool)
      )
    })
    pathBundle.setPathType() // re-set path type

    if (pathBundle.paths.length > 0 && !isMissed) {
      // isUniswapV2Flash
      const path = pathBundle.paths[0]
      // const isUniswapV2Flash = false
      const isUniswapV2Flash =
        pathBundle.paths.length === 1 &&
        path.type === PathType.ALL &&
        path.pools[path.pools.length - 1].protocolGroup === ProtocolGroup.UNISWAP_V2 &&
        balance2 < path.optimalAmount

      transaction.data = pathBundle.toData(targetBlock, isUniswapV2Flash)
      transaction.to = pathBundle.type === PathType.ALL ? contract2.address : contract1.address

      // gas, profit
      const gas =
        pathBundle.paths.length > 1
          ? await this.estimateGas(lastBlock, pathBundle, transaction, pendingTxHash)
          : pathBundle.paths[0].estimatedGas
      const profit = pathBundle.paths.reduce((acc, path) => acc + path.profit, 0)
      const bundleExpenseRatio =
        pathBundle.paths.reduce((acc, path) => acc + path.minExpenseRatio() * path.profit, 0) / profit // weighted path expenseRatio
      // * (pathBundle.paths.length > 1
      //   ? (gas / pathBundle.paths.reduce((acc, path) => acc + path.estimatedGas, 0)) * 1.01
      //   : 1) // realize advantage of multi-path

      // minerReward
      const minExpenseRatio = (baseFee * gas) / profit
      const profitable = minExpenseRatio < 0.99
      const expenseRatio = minExpenseRatio < bundleExpenseRatio ? bundleExpenseRatio : minExpenseRatio
      const minerReward = expenseRatio * profit
      const profitNet = profit - minerReward

      // maxFeePerGas
      // let maxFeePerGas, maxPriorityFeePerGas
      // if (expenseRatio < 0.95) {
      //   maxFeePerGas = profit / gas
      //   maxPriorityFeePerGas = maxFeePerGas - baseFee
      // } else {
      //   maxFeePerGas = minerReward / gas
      //   maxPriorityFeePerGas = maxFeePerGas
      //   // works ok. just not accepted by relay
      //   // if (bundleExpenseRatio < minExpenseRatio) {
      //   //   maxFeePerGas = (profit / gas) * 0.99
      //   //   maxPriorityFeePerGas = 1e-9 // 1 gwei
      //   // }
      // }

      const maxFeePerGas = minerReward / gas
      const maxPriorityFeePerGas = maxFeePerGas

      // isEnoughFundsForGas
      const isEnoughFundsForGas = walletBalance > maxFeePerGas * gas
      if (!isEnoughFundsForGas) pathBundle.logger.warn('Not enough funds for gas')

      if (profitable && isEnoughFundsForGas) {
        const logArr = [
          // `Amount:${path.optimalAmount.toFixed(8)}`,
          `Profit:${profit.toFixed(8)}`,
          `ProfitNet:${profitNet.toFixed(8)}`,
          `MinerReward:${minerReward.toFixed(8)}`,
          `MaxFeePerGas:${(maxFeePerGas * 1e9).toFixed(9)}`,
          `MaxPriorityFeePerGas:${(maxPriorityFeePerGas * 1e9).toFixed(9)}`,
          `ExpenseRatio:${expenseRatio}`,
          `Gas:${gas}`,
        ]
        if (sim) logArr.push(`Sim:${sim}`)
        let log = logArr.join(', ')
        if (sim) log = chalk.bold(log)

        pathBundle.logger.log(log)

        transaction.maxFeePerGas = utils.parseEther(maxFeePerGas.toFixed(18))
        transaction.maxPriorityFeePerGas = utils.parseEther(maxPriorityFeePerGas.toFixed(18))
        transaction.gasLimit = Math.round(gas * 1.2)
        if (this.lastTargetBlock === targetBlock) {
          this.logger.warn(`TargetBlock ${targetBlock} is busy`)
          return
        }

        this.lastTargetBlock = targetBlock

        const signedTransaction = await wallet.signTransaction(transaction)
        const tx = await flashbots.sendBundle([{ signedTransaction }], targetBlock)
        if (tx && 'error' in tx) {
          this.logger.error('BundleError: ' + (tx as RelayResponseError).error.message)
        } else if (tx) {
          ;(tx as FlashbotsTransactionResponse)
            .wait()
            .then(async resolution => {
              let type: LogType
              const status = FlashbotsBundleResolution[resolution]
              const baseFeeB = (await this.chainService.getBaseFee(targetBlock)) || BigNumber.from('0')
              const baseFee = parseFloat(utils.formatEther(baseFeeB))
              const baseFeeGwei = parseFloat(utils.formatUnits(baseFeeB, 'gwei'))
              const isBlockFeeSatisfy = (transaction.maxFeePerGas as BigNumber).gt(baseFeeB)

              if (resolution === FlashbotsBundleResolution.BundleIncluded) {
                type = LogType.BUNDLE_INCLUDED
                const txHash = (await (tx as FlashbotsTransactionResponse).receipts())[0].transactionHash
                const receipt = await this.provider.getTransactionReceipt(txHash)
                const gasUsed = parseInt(receipt.gasUsed.toString())
                const effectiveGasPrice = parseFloat(utils.formatEther(receipt.effectiveGasPrice))
                const actualProfit = profit - gasUsed * effectiveGasPrice
                pathBundle.logger.log(
                  [status, `BaseFee: ${baseFeeGwei}`, `GasUsed: ${gasUsed}`, `ActualProfit: ${actualProfit}`].join(', ')
                )
              } else {
                type = LogType.BUNDLE_NOT_INCLUDED
                pathBundle.logger.error([status, `baseFee: ${isBlockFeeSatisfy}`].join(', '))
                if (sim) {
                  // since response may be a little bit earlier than block update data
                  setTimeout(() => {
                    this.onPending(pendingTxHash)
                  }, 100)
                }
              }

              await this.logModel.create({
                blockNumber: lastBlock,
                pathId: pathBundle.paths[0].id,
                type,
                message: status,
                swapParams: transaction.data,
                other: {
                  profit,
                  profitNet,
                  expenseRatio,
                  gas,
                  gasLimit: transaction.gasLimit,
                  maxFeePerGas: parseFloat(utils.formatUnits(transaction.maxFeePerGas, 'gwei')),
                  maxPriorityFeePerGas: parseFloat(utils.formatUnits(transaction.maxPriorityFeePerGas, 'gwei')),
                  targetBlock,
                  targetBlockBaseFee: baseFeeGwei,
                  isBlockFeeSatisfy,
                  sim,
                },
              })
            })
            .catch(e => this.logger.error(e))
        } else {
          this.logger.error('No tx response')
          process.exit(-1)
        }
      }
    }
  }
}
