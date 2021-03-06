# -*- coding: utf-8 -*-

import logging
import time

from apscheduler.schedulers.background import BackgroundScheduler

from zvdata import IntervalLevel
from zvt import init_log
from zvt.recorders.eastmoney.dividend_financing.dividend_detail_recorder import DividendDetailRecorder
from zvt.recorders.eastmoney.dividend_financing.dividend_financing_recorder import DividendFinancingRecorder
from zvt.recorders.eastmoney.dividend_financing.rights_issue_detail_recorder import RightsIssueDetailRecorder
from zvt.recorders.eastmoney.dividend_financing.spo_detail_recorder import SPODetailRecorder
from zvt.recorders.eastmoney.finance.china_stock_cash_flow_recorder import ChinaStockCashFlowRecorder
from zvt.recorders.eastmoney.finance.china_stock_finance_factor_recorder import ChinaStockFinanceFactorRecorder
from zvt.recorders.eastmoney.finance.china_stock_income_statement_recorder import ChinaStockIncomeStatementRecorder
from zvt.recorders.eastmoney.holder.top_ten_holder_recorder import TopTenHolderRecorder
from zvt.recorders.eastmoney.holder.top_ten_tradable_holder_recorder import TopTenTradableHolderRecorder
from zvt.recorders.eastmoney.meta.china_stock_category_recorder import EastmoneyChinaBlockStockRecorder
from zvt.recorders.eastmoney.meta.china_stock_meta_recorder import EastmoneyChinaStockDetailRecorder
from zvt.recorders.eastmoney.quotes.china_stock_kdata_recorder import ChinaStockKdataRecorder
from zvt.recorders.eastmoney.finance.china_stock_balance_sheet_recorder import ChinaStockBalanceSheetRecorder

logger = logging.getLogger(__name__)
sched = BackgroundScheduler()


@sched.scheduled_job('cron', hour=2, minute=00, day_of_week='sat')
def dividend_run():
    loop = 8
    while loop >= 0:
        try:
            DividendFinancingRecorder().run()
            RightsIssueDetailRecorder().run()
            SPODetailRecorder().run()
            DividendDetailRecorder().run()

            break
        except Exception as e:
            loop -= 1
            logger.exception('eastmoney dividend_run runner error:{}'.format(e))
            time.sleep(60*(10-loop))


# block temperate
@sched.scheduled_job('cron', hour=1, minute=20, day_of_week='sun')
def finance_run():
    loop = 8
    while loop >= 0:
        try:
            # jointquant.source
            ChinaStockFinanceFactorRecorder().run()
            ChinaStockCashFlowRecorder().run()
            ChinaStockBalanceSheetRecorder().run()
            ChinaStockIncomeStatementRecorder().run()
            break
        except Exception as e:
            loop -= 1
            logger.exception('eastmoney finance runner 0 error:{}'.format(e))
            time.sleep(60*(10-loop))


@sched.scheduled_job('cron', hour=2, minute=30, day_of_week='tue,thu')
def holder_run():
    loop = 8
    while loop >= 0:
        try:
            TopTenHolderRecorder().run()
            TopTenTradableHolderRecorder().run()
            break
        except Exception as e:
            loop -= 1
            logger.exception('eastmoney holder runner error:{}'.format(e))
            time.sleep(60*(10-loop))


@sched.scheduled_job('cron', hour=1, minute=10, day_of_week='sat')
def meta_run():
    loop = 8
    while True:
        try:
            EastmoneyChinaBlockStockRecorder().run()
            EastmoneyChinaStockDetailRecorder().run()
            break
        except Exception as e:
            loop -= 1
            logger.exception('easymoney meta runner error:{}'.format(e))
            time.sleep(60*(10-loop))


# @sched.scheduled_job('cron', hour=16, minute=00, day_of_week='mon-fri')
def quote_run():
    loop = 8
    while loop >= 0:
        try:
            week_kdata = ChinaStockKdataRecorder(level=IntervalLevel.LEVEL_1WEEK)
            week_kdata.run()

            mon_kdata = ChinaStockKdataRecorder(level=IntervalLevel.LEVEL_1MON)
            mon_kdata.run()

            break
        except Exception as e:
            loop -= 1
            logger.exception('easymoney quote runner error:{}'.format(e))
            time.sleep(60*(10-loop))



if __name__ == '__main__':
    init_log('eastmoney_run_recoder.log')

    #quote_run()
    #meta_run()
    #holder_run()
    #finance_run()
    #dividend_run()
    sched.start()
    sched._thread.join()
