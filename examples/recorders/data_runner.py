# -*- coding: utf-8 -*-
import logging
import time

from apscheduler.schedulers.background import BackgroundScheduler

from zvt import init_log
from zvt.domain import *
from zvt.informer.informer import EmailInformer

logger = logging.getLogger(__name__)

sched = BackgroundScheduler()

@sched.scheduled_job('cron', hour=15, minute=30)
def record_kdata():
    while True:
        email_action = EmailInformer()

        try:
            Stock.record_data(provider='joinquant', sleeping_time=1)
            StockTradeDay.record_data(provider='joinquant', sleeping_time=1)
            Stock1dKdata.record_data(provider='joinquant', sleeping_time=1)
            StockValuation.record_data(provider='joinquant', sleeping_time=1)

            email_action.send_message("31591084@qq.com", 'joinquant record kdata finished', '')
            break
        except Exception as e:
            msg = f'joinquant runner error:{e}'
            logger.exception(msg)

            email_action.send_message("31591084@qq.com", 'joinquant runner error', msg)
            time.sleep(60*2)


@sched.scheduled_job('cron', hour=18, minute=30)
def record_others():
    while True:
        #email_action = EmailInformer()

        try:
            Etf.record_data(provider='joinquant', sleeping_time=1)
            EtfStock.record_data(provider='joinquant', sleeping_time=1)

            #email_action.send_message("31591084@qq.com", 'joinquant runner finished', '')
            break
        except Exception as e:
            msg = f'joinquant runner error:{e}'
            logger.exception(msg)

            #email_action.send_message("31591084@qq.com", 'joinquant runner error', msg)
            time.sleep(60*2)


@sched.scheduled_job('cron', hour=15, minute=30, day_of_week='mon,wen,fri')
def record_block():
    while True:
        #email_action = EmailInformer()

        try:
            Block.record_data(provider='sina',sleeping_time=2)
            BlockStock.record_data(provider='sina',sleeping_time=2)

            #email_action.send_message("31591084@qq.com", 'sina block finished', '')
            break
        except Exception as e:
            msg = f'sina block error:{e}'
            logger.exception(msg)

            #email_action.send_message("31591084@qq.com", 'sina block error', msg)
            time.sleep(60*2)

# 自行更改定定时运行时间
# 这些数据都是些低频分散的数据，每天更新一次即可
@sched.scheduled_job('cron', hour=2, minute=00, day_of_week=6)
def record_finance():
    while True:
        #email_action = EmailInformer()

        try:
            Stock.record_data(provider='eastmoney')
            FinanceFactor.record_data(provider='eastmoney')
            BalanceSheet.record_data(provider='eastmoney')
            IncomeStatement.record_data(provider='eastmoney')
            CashFlowStatement.record_data(provider='eastmoney')

            #email_action.send_message("31591084@qq.com", 'eastmoney runner1 finished', '')
            break
        except Exception as e:
            msg = f'eastmoney runner1 error:{e}'
            logger.exception(msg)

            #email_action.send_message("31591084@qq.com", 'eastmoney runner1 error', msg)
            time.sleep(60)

if __name__ == '__main__':
    init_log('data_run.log')

    # 定时启动即可，无需启动时候附带运行一次

    sched.start()

    sched._thread.join()




