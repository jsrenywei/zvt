# -*- coding: utf-8 -*-
import logging
import time

from apscheduler.schedulers.background import BackgroundScheduler

from zvdata import IntervalLevel
from zvt import init_log
from zvt.recorders.joinquant.quotes.jq_stock_kdata_recorder import JqChinaStockKdataRecorder

logger = logging.getLogger(__name__)

sched = BackgroundScheduler()


@sched.scheduled_job('cron', hour=16, minute=0,day_of_week='mon-fri')
def record_day_kdata():
    while True:
        try:
            JqChinaStockKdataRecorder(level=IntervalLevel.LEVEL_1DAY).run()

            break
        except Exception as e:
            logger.exception('joinquant_run_recorder joinquant day_kdata runner error:{}'.format(e))
            time.sleep(60*2)


# 每周6抓取周线和月线数据
@sched.scheduled_job('cron', day_of_week=6, hour=3, minute=0)
def record_wk_kdata():
    while True:
        try:
            JqChinaStockKdataRecorder(level=IntervalLevel.LEVEL_1WEEK).run()
            JqChinaStockKdataRecorder(level=IntervalLevel.LEVEL_1MON).run()

            break
        except Exception as e:
            logger.exception('joinquant_run_recorder joinquant wk_kdata runner error:{}'.format(e))
            time.sleep(60*2)


if __name__ == '__main__':
    init_log('joinquant_run_recorder.log')

    #record_day_kdata()

    #record_wk_kdata()

    sched.start()

    sched._thread.join()
