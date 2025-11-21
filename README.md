News pipeline (Livesquack + BSE)

Structure:
- main.py -> starts backfill (background), watcher and LLM processor
- supervisor/supervisor.py -> optional process supervisor to auto-restart and send telegram alerts
- logs/ contains watcher.log, backfiller.log, livesquack_llm.log, news_pipeline.log

Setup:
- Copy .env-template to .env and fill values.
- Install dependencies: pip install -r requirements.txt
- Run: python main.py
- Or use supervisor: python supervisor/supervisor.py
