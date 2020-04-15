# -*- coding: utf-8 -*-
import logging
import time
from typing import List

from apscheduler.schedulers.background import BackgroundScheduler

import eastmoneypy
from examples.factors.fundamental_selector import FundamentalSelector
from zvdata import IntervalLevel
from zvdata.api import get_entities
from zvdata.utils.time_utils import now_pd_timestamp, to_time_str
from zvt import init_log
from zvt.domain import Block, BlockMoneyFlow, BlockCategory
from zvt.domain import Stock
from zvt.factors.money_flow_factor import BlockMoneyFlowFactor
from zvt.factors.target_selector import TargetSelector
from zvt.informer.informer import EmailInformer

logger = logging.getLogger(__name__)

sched = BackgroundScheduler()


class IndustryBlockSelector(TargetSelector):

    def __init__(self, entity_ids=None, entity_schema=Block, exchanges=None, codes=None, the_timestamp=None,
                 start_timestamp=None, end_timestamp=None, long_threshold=0.8, short_threshold=0.2,
                 level=IntervalLevel.LEVEL_1DAY, provider='sina', block_selector=None) -> None:
        super().__init__(entity_ids, entity_schema, exchanges, codes, the_timestamp, start_timestamp, end_timestamp,
                         long_threshold, short_threshold, level, provider, block_selector)

    def init_factors(self, entity_ids, entity_schema, exchanges, codes, the_timestamp, start_timestamp, end_timestamp,
                     level):
        block_factor = BlockMoneyFlowFactor(start_timestamp=start_timestamp, end_timestamp=end_timestamp,
                                            provider='sina', window=5)
        self.score_factors.append(block_factor)


class ConceptBlockSelector(TargetSelector):

    def __init__(self, entity_ids=None, entity_schema=Block, exchanges=None, codes=None, the_timestamp=None,
                 start_timestamp=None, end_timestamp=None, long_threshold=0.8, short_threshold=0.2,
                 level=IntervalLevel.LEVEL_1DAY, provider='sina', block_selector=None) -> None:
        super().__init__(entity_ids, entity_schema, exchanges, codes, the_timestamp, start_timestamp, end_timestamp,
                         long_threshold, short_threshold, level, provider, block_selector)

    def init_factors(self, entity_ids, entity_schema, exchanges, codes, the_timestamp, start_timestamp, end_timestamp,
                     level):
        block_factor = BlockMoneyFlowFactor(start_timestamp=start_timestamp, end_timestamp=end_timestamp,
                                            provider='sina', window=10, category=BlockCategory.concept.value)
        self.score_factors.append(block_factor)


@sched.scheduled_job('cron', hour=15, minute=50, day_of_week='mon-fri')
def report_block():
    while True:
        error_count = 0
        email_action = EmailInformer(ssl=True)

        try:
            latest_day: BlockMoneyFlow = BlockMoneyFlow.query_data(order=esc(), limit=1,
                                                                   return_type='domain')
            target_date = latest_day[0].timestamp

            msg = ''
            # 行业板块
            industry_block_selector = IndustryBlockSelector(start_timestamp='2020-01-01', long_threshold=0.8)
            industry_block_selector.run()
            industry_long_blocks = industry_block_selector.get_open_long_targets(timestamp=target_date)

            if industry_long_blocks:
                blocks: List[Block] = Block.query_data(provider='sina', entity_ids=industry_long_blocks,
                                                       return_type='domain')

                info = [f'{block.name}({block.code})\n' for block in blocks]
                msg = msg + '行业板块:' + ' '.join(info) + '\n\n'

            # 概念板块
            concept_block_selector = ConceptBlockSelector(start_timestamp='2020-01-01', long_threshold=0.85)
            concept_block_selector.run()
            concept_long_blocks = concept_block_selector.get_open_long_targets(timestamp=target_date)

            if concept_long_blocks:
                blocks: List[Block] = Block.query_data(provider='sina', entity_ids=concept_long_blocks,
                                                       return_type='domain')

                info = [f'{block.name}({block.code})\n' for block in blocks]
                msg = msg + '概念板块' + ' '.join(info) + '\n\n'

            logger.info(msg)
            email_action.send_message('31591084@qq.com', f'{target_date} 资金流入板块评分结果', msg)
            break
        except Exception as e:
            logger.exception('report_block error:{}'.format(e))
            time.sleep(60 * 3)
            error_count = error_count + 1
            if error_count == 10:
                email_action.send_message("31591084@qq.com", f'report_block error',
                                          'report_block error:{}'.format(e))


# 基本面选股 每周一次即可 基本无变化
@sched.scheduled_job('cron', hour=16, minute=0, day_of_week='6')
def report_core_company():
    while True:
        error_count = 0
        email_action = EmailInformer()

        try:
            # StockTradeDay.record_data(provider='joinquant')
            # Stock.record_data(provider='joinquant')
            # FinanceFactor.record_data(provider='eastmoney')
            # BalanceSheet.record_data(provider='eastmoney')

            target_date = to_time_str(now_pd_timestamp())

            my_selector: TargetSelector = FundamentalSelector(start_timestamp='2015-01-01', end_timestamp=target_date)
            my_selector.run()

            long_targets = my_selector.get_open_long_targets(timestamp=target_date)
            if long_targets:
                stocks = get_entities(provider='joinquant', entity_schema=Stock, entity_ids=long_targets,
                                      return_type='domain')

                # add them to eastmoney
                try:
                    try:
                        eastmoneypy.del_group('core')
                    except:
                        pass
                    eastmoneypy.create_group('core')
                    for stock in stocks:
                        eastmoneypy.add_to_group(stock.code, group_name='core')
                except Exception as e:
                    email_action.send_message("31591084@qq.com", f'report_core_company error',
                                              'report_core_company error:{}'.format(e))

                info = [f'{stock.name}({stock.code})\n' for stock in stocks]
                msg = ' '.join(info)
            else:
                msg = 'no targets'

            logger.info(msg)

            email_action.send_message("31591084@qq.com", f'{to_time_str(target_date)} 核心资产选股结果', msg)
            break
        except Exception as e:
            logger.exception('report_core_company error:{}'.format(e))
            time.sleep(60 * 3)
            error_count = error_count + 1
            if error_count == 10:
                email_action.send_message("31591084@qq.com", f'report_core_company error',
                                          'report_core_company error:{}'.format(e))


@sched.scheduled_job('cron', hour=18, minute=0, day_of_week='mon-fri')
def report_cross_ma():
    while True:
        error_count = 0
        email_action = EmailInformer()

        try:
            # 抓取k线数据
            # StockTradeDay.record_data(provider='joinquant')
            # Stock1dKdata.record_data(provider='joinquant')

            latest_day: StockTradeDay = StockTradeDay.query_data(order=StockTradeDay.timestamp.desc(), limit=1,
                                                                 return_type='domain')
            if latest_day:
                target_date = latest_day[0].timestamp
            else:
                target_date = now_pd_timestamp()

            # 计算均线
            my_selector = TargetSelector(start_timestamp='2018-01-01', end_timestamp=target_date)
            # add the factors
            ma_factor = CrossMaFactor(start_timestamp='2018-01-01', end_timestamp=target_date)

            my_selector.add_filter_factor(ma_factor)

            my_selector.run()

            long_targets = my_selector.get_open_long_targets(timestamp=target_date)
            if long_targets:
                stocks = get_entities(provider='joinquant', entity_schema=Stock, entity_ids=long_targets,
                                      return_type='domain')
                info = [f'{stock.name}({stock.code})\n' for stock in stocks]
                msg = ' '.join(info)
            else:
                msg = 'no targets'

            logger.info(msg)

            email_action.send_message("31591084@qq.com", f'{target_date} 均线选股结果', msg)

            break
        except Exception as e:
            logger.exception('report_cross_ma error:{}'.format(e))
            time.sleep(60 * 3)
            error_count = error_count + 1
            if error_count == 10:
                email_action.send_message("31591084@qq.com", f'report_cross_ma error',
                                          'report_cross_ma error:{}'.format(e))


@sched.scheduled_job('cron', hour=17, minute=30, day_of_week='mon-fri')
def report_real():
    while True:
        error_count = 0
        email_action = EmailInformer(ssl=True)

        try:
            latest_day: Stock1dKdata = Stock1dKdata.query_data(order=Stock1dKdata.timestamp.desc(), limit=1,
                                                               return_type='domain')
            target_date = latest_day[0].timestamp
            # target_date = '2020-02-04'

            # 计算均线
            my_selector = TargetSelector(start_timestamp='2018-01-01', end_timestamp=target_date)
            # add the factors
            factor1 = VolumeUpMa250Factor(start_timestamp='2018-01-01', end_timestamp=target_date)

            my_selector.add_filter_factor(factor1)

            my_selector.run()

            long_stocks = my_selector.get_open_long_targets(timestamp=target_date)

            msg = 'no targets'
            # 过滤亏损股
            # check StockValuation data
            pe_date = target_date - datetime.timedelta(10)
            if StockValuation.query_data(start_timestamp=pe_date, limit=1, return_type='domain'):
                positive_df = StockValuation.query_data(provider='joinquant', entity_ids=long_stocks,
                                                        start_timestamp=pe_date,
                                                        filters=[StockValuation.pe > 0],
                                                        columns=['entity_id'])
                bad_stocks = set(long_stocks) - set(positive_df['entity_id'].tolist())
                if bad_stocks:
                    stocks = get_entities(provider='joinquant', entity_schema=Stock, entity_ids=bad_stocks,
                                          return_type='domain')
                    info = [f'{stock.name}({stock.code})\n' for stock in stocks]
                    msg = '亏损股:' + ' '.join(info) + '\n\n'

                long_stocks = set(positive_df['entity_id'].tolist())

            if long_stocks:
                # use block to filter
                block_selector = BlockSelector(start_timestamp='2020-01-01', long_threshold=0.8)
                block_selector.run()
                long_blocks = block_selector.get_open_long_targets(timestamp=target_date)

                if long_blocks:
                    blocks: List[Block] = Block.query_data(provider='sina', entity_ids=long_blocks,
                                                           return_type='domain')

                    info = [f'{block.name}({block.code})\n' for block in blocks]
                    msg = ' '.join(info) + '\n\n'

                    block_stocks: List[BlockStock] = BlockStock.query_data(provider='sina',
                                                                           filters=[
                                                                               BlockStock.stock_id.in_(long_stocks)],
                                                                           entity_ids=long_blocks, return_type='domain')
                    if block_stocks:
                        # add them to eastmoney
                        try:
                            try:
                                eastmoneypy.del_group('real')
                            except:
                                pass
                            eastmoneypy.create_group('real')
                            for block_stock in block_stocks:
                                eastmoneypy.add_to_group(block_stock.stock_code, group_name='real')
                        except Exception as e:
                            email_action.send_message("31591084@qq.com", f'report_real error',
                                                      'report_real error:{}'.format(e))

                        block_map_stocks = {}
                        for block_stock in block_stocks:
                            stocks = block_map_stocks.get(block_stock.name)
                            if not stocks:
                                stocks = []
                                block_map_stocks[block_stock.name] = stocks
                            stocks.append(f'{block_stock.stock_name}({block_stock.stock_code})\n')

                        for block in block_map_stocks:
                            stocks = block_map_stocks[block]
                            stock_msg = ' '.join(stocks)
                            msg = msg + f'{block}:\n' + stock_msg + '\n\n'

            logger.info(msg)
            email_action.send_message('31591084@qq.com', f'{target_date} 放量突破年线real选股结果', msg)
            break
        except Exception as e:
            logger.exception('report_real error:{}'.format(e))
            time.sleep(60 * 3)
            error_count = error_count + 1
            if error_count == 10:
                email_action.send_message("31591084@qq.com", f'report_real error',
                                          'report_real error:{}'.format(e))


@sched.scheduled_job('cron', hour=17, minute=30, day_of_week='mon-fri')
def report_state():
    while True:
        error_count = 0
        email_action = EmailInformer(ssl=True)

        try:
            latest_day: Stock1dKdata = Stock1dKdata.query_data(order=Stock1dKdata.timestamp.desc(), limit=1,
                                                               return_type='domain')
            target_date = latest_day[0].timestamp
            # target_date = to_pd_timestamp('2020-01-02')

            # 计算均线
            my_selector = TargetSelector(start_timestamp='2018-01-01', end_timestamp=target_date)
            # add the factors
            factor1 = VolumeUpMa250Factor(start_timestamp='2018-01-01', end_timestamp=target_date)

            my_selector.add_filter_factor(factor1)

            my_selector.run()

            long_stocks = my_selector.get_open_long_targets(timestamp=target_date)

            msg = 'no targets'
            # 过滤亏损股
            # check StockValuation data
            pe_date = target_date - datetime.timedelta(10)
            if StockValuation.query_data(start_timestamp=pe_date, limit=1, return_type='domain'):
                positive_df = StockValuation.query_data(provider='joinquant', entity_ids=long_stocks,
                                                        start_timestamp=pe_date,
                                                        filters=[StockValuation.pe > 0],
                                                        columns=['entity_id'])
                bad_stocks = set(long_stocks) - set(positive_df['entity_id'].tolist())
                if bad_stocks:
                    stocks = get_entities(provider='joinquant', entity_schema=Stock, entity_ids=bad_stocks,
                                          return_type='domain')
                    info = [f'{stock.name}({stock.code})\n' for stock in stocks]
                    msg = '亏损股:' + ' '.join(info) + '\n\n'

                long_stocks = set(positive_df['entity_id'].tolist())

            if long_stocks:
                pre_date = target_date - datetime.timedelta(3 * 365)
                ma_state = MaStateStatsFactor(entity_ids=long_stocks, start_timestamp=pre_date,
                                              end_timestamp=target_date, persist_factor=False)
                bad_stocks = []
                for entity_id, df in ma_state.factor_df.groupby(level=0):
                    if df['current_pct'].max() >= 0.35:
                        bad_stocks.append(entity_id)
                        long_stocks.remove(entity_id)
                if bad_stocks:
                    stocks = get_entities(provider='joinquant', entity_schema=Stock, entity_ids=bad_stocks,
                                          return_type='domain')
                    info = [f'{stock.name}({stock.code})\n' for stock in stocks]
                    msg = msg + '3年内高潮过:' + ' '.join(info) + '\n\n'

            # 过滤风险股
            if long_stocks:
                risky_codes = risky_company(the_date=target_date, entity_ids=long_stocks)

                if risky_codes:
                    long_stocks = [entity_id for entity_id in long_stocks if
                                   get_entity_code(entity_id) not in risky_codes]

                    stocks = get_entities(provider='joinquant', entity_schema=Stock, entity_ids=risky_codes,
                                          return_type='domain')
                    info = [f'{stock.name}({stock.code})\n' for stock in stocks]
                    msg = msg + '风险股:' + ' '.join(info) + '\n\n'
            if long_stocks:
                stocks = get_entities(provider='joinquant', entity_schema=Stock, entity_ids=long_stocks,
                                      return_type='domain')
                # add them to eastmoney
                try:
                    try:
                        eastmoneypy.del_group('real')
                    except:
                        pass
                    eastmoneypy.create_group('real')
                    for stock in stocks:
                        eastmoneypy.add_to_group(stock.code, group_name='real')
                except Exception as e:
                    email_action.send_message("31591084@qq.com", f'report state error',
                                              'report state error:{}'.format(e))

                info = [f'{stock.name}({stock.code})\n' for stock in stocks]
                msg = msg + '盈利股:' + ' '.join(info) + '\n\n'

            logger.info(msg)
            email_action.send_message('31591084@qq.com', f'{target_date} 放量突破年线state选股结果', msg)
            break
        except Exception as e:
            logger.exception('report state error:{}'.format(e))
            time.sleep(60 * 3)
            error_count = error_count + 1
            if error_count == 10:
                email_action.send_message("31591084@qq.com", f'report state error',
                                          'report state error:{}'.format(e))


@sched.scheduled_job('cron', hour=17, minute=0, day_of_week='mon-fri')
def report_vol_up_250():
    while True:
        error_count = 0
        email_action = EmailInformer()

        try:
            # 抓取k线数据
            # StockTradeDay.record_data(provider='joinquant')
            # Stock1dKdata.record_data(provider='joinquant')

            latest_day: Stock1dKdata = Stock1dKdata.query_data(order=Stock1dKdata.timestamp.desc(), limit=1,
                                                               return_type='domain')
            target_date = latest_day[0].timestamp

            # 计算均线
            my_selector = TargetSelector(start_timestamp='2018-01-01', end_timestamp=target_date)
            # add the factors
            factor1 = VolumeUpMa250Factor(start_timestamp='2018-01-01', end_timestamp=target_date)

            my_selector.add_filter_factor(factor1)

            my_selector.run()

            long_stocks = my_selector.get_open_long_targets(timestamp=target_date)

            msg = 'no targets'

            # 过滤亏损股
            # check StockValuation data
            pe_date = target_date - datetime.timedelta(10)
            if StockValuation.query_data(start_timestamp=pe_date, limit=1, return_type='domain'):
                positive_df = StockValuation.query_data(provider='joinquant', entity_ids=long_stocks,
                                                        start_timestamp=pe_date,
                                                        filters=[StockValuation.pe > 0],
                                                        columns=['entity_id'])
                bad_stocks = set(long_stocks) - set(positive_df['entity_id'].tolist())
                if bad_stocks:
                    stocks = get_entities(provider='joinquant', entity_schema=Stock, entity_ids=bad_stocks,
                                          return_type='domain')
                    info = [f'{stock.name}({stock.code})\n' for stock in stocks]
                    msg = '亏损股:' + ' '.join(info) + '\n\n'

                long_stocks = set(positive_df['entity_id'].tolist())

            if long_stocks:
                stocks = get_entities(provider='joinquant', entity_schema=Stock, entity_ids=long_stocks,
                                      return_type='domain')
                # add them to eastmoney
                try:
                    try:
                        eastmoneypy.del_group('tech')
                    except:
                        pass
                    eastmoneypy.create_group('tech')
                    for stock in stocks:
                        eastmoneypy.add_to_group(stock.code, group_name='tech')
                except Exception as e:
                    email_action.send_message("31591084@qq.com", f'report_vol_up_250 error',
                                              'report_vol_up_250 error:{}'.format(e))

                info = [f'{stock.name}({stock.code})\n' for stock in stocks]
                msg = msg + '盈利股:' + ' '.join(info) + '\n\n'

            logger.info(msg)

            email_action.send_message("31591084@qq.com", f'{target_date} 放量突破年线选股结果', msg)

            break
        except Exception as e:
            logger.exception('report_vol_up_250 error:{}'.format(e))
            time.sleep(60 * 3)
            error_count = error_count + 1
            if error_count == 10:
                email_action.send_message("31591084@qq.com", f'report_vol_up_250 error',
                                          'report_vol_up_250 error:{}'.format(e))


if __name__ == '__main__':
    init_log('report.log')

    report_block()

    sched.start()

    sched._thread.join()
