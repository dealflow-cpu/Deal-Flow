# DealFlow Montana

Real Montana business-for-sale listings scraped from 15+ broker sites every 4 hours. Every listing links to the actual ad.

## Sources
BizBuySell · BizQuest · BusinessBroker.net · DealStream · LoopNet · GlobalBX · Montana 406 Brokers · Synergy BB · Murphy Business · Transworld · Sunbelt · CBC Montana · BusinessesForSale.com · and more

## Setup (from your iPhone)

### Step 1: You're here
You already have the code on GitHub. ✅

### Step 2: Deploy to Render (free)
1. Open **render.com** in Safari
2. Sign up with your GitHub account
3. Tap **"New"** → **"Blueprint"**
4. Connect this `dealflow` repo
5. Render reads the `render.yaml` and creates everything automatically
6. Wait 2-3 minutes for it to build

You now have:
- A live app at `https://dealflow-XXXX.onrender.com`
- A scraper that runs every 4 hours automatically

### Step 3: Add to your Home Screen
1. Open your Render app URL in Safari
2. Tap the **Share button** (square with arrow)
3. Tap **"Add to Home Screen"**
4. Name it **DealFlow**
5. Tap **Add**

Done. You have a real app on your phone with real listings.

### Step 4: First scrape
The cron job runs every 4 hours. To trigger the first scrape immediately:
- Visit `https://your-app.onrender.com/status` to check if data exists
- The first cron run will populate your data

## Files
- `scraper.py` — Mega scraper (15+ sources)
- `server.py` — Flask server (serves app + data)
- `index.html` — Frontend app
- `render.yaml` — Render deployment config
- `requirements.txt` — Python dependencies
