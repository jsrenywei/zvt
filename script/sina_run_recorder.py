# -*- coding: utf-8 -*-
import logging
import time

from apscheduler.schedulers.background import BackgroundScheduler

from zvt import init_log
from zvt.recorders.sina.meta.sina_china_stock_category_recorder import SinaChinaBlockRecorder
from zvt.recorders.sina.money_flow import SinaBlockMoneyFlowRecorder

logger = logging.getLogger(__name__)

sched = BackgroundScheduler()


@sched.scheduled_job('cron', hour=2, minute=00, day_of_week='tue-sat')
def run():
    loop = 8
    while loop >= 0:
        try:
            SinaChinaBlockRecorder().run()

            SinaBlockMoneyFlowRecorder().run()
            break
        except Exception as e:
            loop -= 1
            logger.exception('sina runner error:{}'.format(e))
            time.sleep(60*(10-loop))


if __name__ == '__main__':
    init_log('sina_run_recorder.log')
    #run()

    sched.start()
    sched._thread.join()
