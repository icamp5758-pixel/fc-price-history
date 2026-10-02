# FC Online price history

Automated price-history collector for the FC Online mobile DataCenter.

## First run
The crawler seeds `price_history.json` from the existing public history when the file is absent, preserves player metadata/history, then appends graph observations for enhancement grades 8–11.

GitHub Actions runs every two hours and can also be started manually from Actions → Update FC Online price history → Run workflow.

## Safety
The script refuses to overwrite the history if the loaded player universe is unexpectedly small.
