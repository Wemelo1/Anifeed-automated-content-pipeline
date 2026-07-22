# AniFeed Studio — Windows setup & migration

Run everything from the project folder in **PowerShell** (not cmd).

## A. On the PHONE (Termux) — freeze the state
1. Let any running pipeline finish, or stop it. Stop cron/Termux:Boot jobs too.
2. Flush the SQLite WAL into the main file:
   ```
   cd ~/storage/shared/tiktok
   python -c "import sqlite3; c=sqlite3.connect('pipeline.db'); print(c.execute('PRAGMA wal_checkpoint(TRUNCATE)').fetchall()); c.close()"
   ls pipeline.db*          # pipeline.db-wal should be gone or 0 bytes
   ```
3. Pack into ONE archive (copying thousands of small files over USB/MTP is painfully slow):
   ```
   pkg install zip
   cd ~/storage/shared && zip -r tiktok_backup.zip tiktok
   ```
   Includes pipeline.db, .env, youtube_token.json, client_secret.json, bgm/ music/ sfx/, fonts, downloads/ edited/ finished/.
4. Copy `tiktok_backup.zip` to the laptop over USB. **Keep the phone folder untouched for a week as your backup.**
5. **From now on never run the phone and laptop against separate copies of the DB.** Disable the phone's cron/Boot jobs.

## B. On the laptop — install
```powershell
# unzip to a SHORT path with no apostrophes, e.g.:
Expand-Archive $HOME\Downloads\tiktok_backup.zip C:\
Rename-Item C:\tiktok C:\anifeed ; cd C:\anifeed

python --version                       # 3.10+ ; install from python.org if missing
python -m venv .venv
.\.venv\Scripts\Activate.ps1           # if blocked: Set-ExecutionPolicy -Scope CurrentUser RemoteSigned
pip install -r requirements-windows.txt

setx PYTHONUTF8 1                      # then CLOSE and reopen PowerShell
git init ; git add . ; git status      # confirm .env / pipeline.db / tokens are NOT listed
```

## C. Verify BEFORE running anything
```powershell
python windows_selfcheck.py
```
Fix every `FAIL`. If check **A** (burn_subtitles shape) fails but **A'** passes, the fix is adding `fontsdir=C:/Windows/Fonts` (via `ffmpeg_filter_path`) in burn_subtitles.py — tell me.
Check **D** is *expected* to say "BREAKS" on Windows — that's the drive-letter bug the helper fixes.

Then, once the live files are patched: `python run_all.py --list`, then `python run_all.py pipeline --only rss_news_pull` as a dry first run, then the full thing.

## D. Run
```powershell
python run_all.py                 # pipeline
python run_all.py analytics
python run_all.py weekly
python run_all.py --only edit_clips,burn_subtitles
python run_all.py --list
python run_all.py --force         # only if a crashed run left logs\run.lock
```
Logs: `logs\run_*.log` (last 30). HDD timing: `logs\stage_timings.csv`.

## E. Keep it running with the lid closed
Keep-awake in run_all.py stops *idle sleep* only. Lid-close needs this (once):
```powershell
powercfg /setacvalueindex SCHEME_CURRENT SUB_BUTTONS LIDACTION 0
powercfg /setdcvalueindex SCHEME_CURRENT SUB_BUTTONS LIDACTION 0
powercfg /setactive SCHEME_CURRENT
```
(Do-nothing on lid close. Keep it plugged in and ventilated; it's an old laptop.)

## F. Optional: Task Scheduler (use the venv's python by full path)
```powershell
$py="C:\anifeed\.venv\Scripts\python.exe"; $ra="C:\anifeed\run_all.py"
schtasks /Create /TN "AniFeed Pipeline"  /SC DAILY  /ST 06:00 /TR "`"$py`" `"$ra`" pipeline"  /F
schtasks /Create /TN "AniFeed Analytics" /SC DAILY  /ST 05:00 /TR "`"$py`" `"$ra`" analytics" /F
schtasks /Create /TN "AniFeed Weekly"    /SC WEEKLY /D SUN /ST 08:00 /TR "`"$py`" `"$ra`" weekly" /F
```
Tasks run only while you're logged in by default. Closing the console window kills a run without cleanup → `logs\run.lock` goes stale after 4 h (or use `--force`).

## G. After the move
- `python check_missing_finished.py` (dry run), review, then `--apply` (12 orphaned clips).
- Never manually delete `downloads/` or `finished/`; use `cleanup_used_downloads.py` (dry run first).
