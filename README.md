# Gold (XAUT) options paper trader

Delta Exchange India ke gold options pe strategy ka live paper trading.
Roz GitHub Actions 19:45 IST pe chalta hai, entry window (expiry se 60 min pehle)
me paper trades karta hai, aur 21:30 IST expiry settle hone ke baad band ho jaata hai.

- Dashboard: GitHub Pages (Settings -> Pages -> branch `main`, folder `/docs`)
- Trades: `paper_trades.csv`
- Settings badalni ho to `.github/workflows/paper-trade.yml` me
  `python gold_paper_trader.py ...` line me options jodo, jaise `--band 10 --target-x 1000`.

Sirf paper trading hai. Koi asli order nahi lagta.
