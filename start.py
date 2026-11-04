import asyncio
import logging
import os

from aiohttp import web

import main as bot_main

# ============================================================================
# start.py
#
# این فایل خودش ربات نیست؛ فقط یک سرور HTTP کوچیکه که Render (یا هر
# پلتفرم مشابه) برای "روشن نگه‌داشتن" سرویس بهش نیاز داره (healthCheckPath).
# منطق واقعی ربات (polling به api.splus.ir، دیتابیس، همهٔ هندلرها) همونجایی
# که بود می‌مونه: main.py. start.py فقط main.main() رو به‌عنوان یک تسک
# پس‌زمینه روشن می‌کنه و همزمان یه اندپوینت /health سرپا نگه می‌داره.
#
# اجرا:   python start.py
# (دیگه مستقیم python main.py اجرا نکن؛ render.yaml هم به همین فایل اشاره
# می‌کنه.)
# ============================================================================

log = logging.getLogger("start")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

PORT = int(os.environ.get("PORT", "10000"))


async def health(request: web.Request) -> web.Response:
    """اندپوینتی که Render باهاش چک می‌کنه سرویس بالاست یا نه. هیچ ربطی به
    وبهوک واقعی تلگرام/سروش نداره؛ ربات همچنان با long polling کار می‌کنه."""
    task = request.app.get("bot_task")
    bot_alive = bool(task and not task.done())
    return web.json_response(
        {"status": "ok" if bot_alive else "starting", "polling": bot_alive}
    )


async def start_bot_polling(app: web.Application):
    """وقتی سرور HTTP بالا اومد، ربات رو به حالت polling توی یک تسک
    پس‌زمینه روشن می‌کنه. اگه تسک قبلی بخاطر ری‌لود/ری‌استارت هنوز زنده
    بود، اول لغوش می‌کنیم تا دو نسخه از polling همزمان راه نیفته."""
    old = app.get("bot_task")
    if old and not old.done():
        old.cancel()

    async def run_and_log():
        try:
            await bot_main.main()
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("ربات با خطا متوقف شد؛ تسک پس‌زمینه تمام شد.")

    app["bot_task"] = asyncio.ensure_future(run_and_log())
    log.info("ربات به‌صورت polling در پس‌زمینه روشن شد.")


async def stop_bot_polling(app: web.Application):
    task = app.get("bot_task")
    if task and not task.done():
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
    session = getattr(bot_main, "_http_session", None)
    if session and not session.closed:
        await session.close()


def build_app() -> web.Application:
    app = web.Application()
    app.router.add_get("/", health)
    app.router.add_get("/health", health)
    app.on_startup.append(start_bot_polling)
    app.on_cleanup.append(stop_bot_polling)
    return app


if __name__ == "__main__":
    web.run_app(build_app(), host="0.0.0.0", port=PORT)
