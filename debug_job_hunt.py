import os
import traceback
from dotenv import load_dotenv

load_dotenv()

from uk_sponsor_mech_bot import must_env, send_admin_debug, main


def debug_run():
    bot_token = must_env("TELEGRAM_BOT_TOKEN")

    try:
        send_admin_debug(bot_token, "🔎 Debug run started")
        main()
        send_admin_debug(bot_token, "✅ Debug run finished")
    except Exception as e:
        msg = f"❌ Debug run failed: {type(e).__name__}: {e}"
        send_admin_debug(bot_token, msg)
        raise


if __name__ == "__main__":
    debug_run()
