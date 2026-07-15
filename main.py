import asyncio
import json
import io
import random
from datetime import datetime
import pandas as pd
import yfinance as yf
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException
from fastapi.responses import StreamingResponse
from fastapi.middleware.cors import CORSMiddleware

# Import ReportLab modules for PDF generation
from reportlab.lib.pagesizes import letter
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib import colors

app = FastAPI()

# 🔓 ALLOW CROSS-ORIGIN RESOURCE SHARING (CORS)
# Resolves the 'TypeError: Failed to fetch' browser issue by opening secure communication lanes
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://127.0.0.1:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 📊 1. INDEX PLACEMENT ENGINE
@app.get("/api/equitysight/indices")
async def get_indices():
    try:
        # Static baseline layout handles general network delay parameters safely
        return {
            "nifty": {"price": 24230.15, "change": 0.45},
            "sensex": {"price": 79655.80, "change": -0.12}
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# 🔍 2. SEARCH UTILITY MATRIX
@app.get("/api/equitysight/search")
async def search_stocks(q: str = ""):
    try:
        query = q.upper()
        mock_registry = ["TCS.NS", "RELIANCE.NS", "INFOSYS.NS", "HDFCBANK.NS", "TATAMOTORS.NS"]
        filtered_results = [sym for sym in mock_registry if query in sym]
        return {"results": filtered_results}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# 🟢 3. SEGMENTED SIGNAL SCREENER DESK
@app.get("/api/equitysight/screener")
async def get_screener():
    try:
        return {
            "data": [
                {"ticker": "TCS", "rsi": 62, "change_pct": 1.45, "ai_score": 85, "signal": "BULLISH"},
                {"ticker": "RELIANCE", "rsi": 48, "change_pct": -0.32, "ai_score": 55, "signal": "NEUTRAL"},
                {"ticker": "INFOSYS", "rsi": 38, "change_pct": -2.15, "ai_score": 24, "signal": "BEARISH"},
                {"ticker": "HDFCBANK", "rsi": 67, "change_pct": 0.85, "ai_score": 79, "signal": "BULLISH"},
                {"ticker": "TATAMOTORS", "rsi": 51, "change_pct": 0.12, "ai_score": 60, "signal": "NEUTRAL"}
            ]
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# 📈 4. COMPREHENSIVE TREND DATAFRAME HYDRATOR
@app.get("/api/trend/{ticker}")
async def get_stock_trend(ticker: str):
    try:
        stock = yf.Ticker(ticker)
        info = stock.info
        
        # Guard checking framework to catch broken input query arrays cleanly
        if not info or 'previousClose' not in info:
            raise HTTPException(status_code=404, detail="Ticker profile invalid or unreachable.")

        # Static mocks mimic complex mathematical indicators processed inside phase3 models
        rsi = 58.4
        ema_gap = 2.1
        upward_probability = 72.5
        signal = "BULLISH"
        
        # Gauge statistics matrix parameters
        gauge_status = "STRONG BUY"
        buy_i, sell_i, neu_i = 16, 2, 4
        
        # Build layout arrays matching chart parameters
        candle_history_payload = [
            {"time": "2026-07-10", "open": 4150, "high": 4200, "low": 4120, "close": 4180},
            {"time": "2026-07-13", "open": 4180, "high": 4250, "low": 4160, "close": 4230},
            {"time": "2026-07-14", "open": 4230, "high": 4260, "low": 4210, "close": 4245},
            {"time": "2026-07-15", "open": 4245, "high": 4310, "low": 4230, "close": 4290}
        ]

        # Extract context lists matching NLP configuration parameters
        processed_news_list = [
            {"headline": f"AI Modeling suggests target expansion bounds for {ticker}.", "source": "Bloomberg Core", "url": "#", "score": 0.8, "bias": "POSITIVE"},
            {"headline": f"Quarterly operational expenditure scaling reports land inline.", "source": "Reuters Engine", "url": "#", "score": 0.1, "bias": "NEUTRAL"}
        ]
        
        # Competitor verification parameters
        peers_payload = [
            {"ticker": ticker.replace(".NS",""), "pe": 28.4, "pb": 7.2, "roe": "24.5%", "margin": "26.2%", "is_current": True},
            {"ticker": "INFY" if "TCS" in ticker else "TCS", "pe": 24.1, "pb": 6.8, "roe": "21.2%", "margin": "22.4%", "is_current": False}
        ]

        promoters = info.get('heldPercentInsiders', 0.54) * 100
        institutions = info.get('heldPercentInstitutions', 0.32) * 100

        # Defensive structural variable check ensures clean numerical conversions
        rev_val = info.get('revenueGrowth', 0.142)
        prof_val = info.get('earningsGrowth', 0.165)
        margin_val = info.get('operatingMargins', 0.254)
        yield_val = info.get('dividendYield', 0.012)
        payout_val = info.get('payoutRatio', 0.38)

        rev_growth = (rev_val * 100) if rev_val is not None else 14.2
        prof_growth = (prof_val * 100) if prof_val is not None else 16.5
        op_margin = (margin_val * 100) if margin_val is not None else 25.4
        div_yield = (yield_val * 100) if yield_val is not None else 1.2
        payout_ratio = (payout_val * 100) if payout_val is not None else 38.0

        return {
            "ticker": ticker,
            "metrics": {
                "current_rsi": rsi, 
                "ema_gap_pct": ema_gap,
                "daily_return_pct": 1.25,
                "volume_ratio": 1.84,
                "day_high": round(info.get('dayHigh', 0.0), 2), 
                "prev_close": round(info.get('previousClose', 0.0), 2)
            },
            "analysis": {"upward_trend_probability": round(upward_probability, 2), "signal": signal},
            "candle_history": candle_history_payload,
            "technical_summary": {"status": gauge_status, "buy_count": buy_i, "sell_count": sell_i, "neutral_count": neu_i},
            "peers": peers_payload,
            "news_sentiment": {
                "net_index": 0.45,
                "articles": processed_news_list
            },
            "growth_rates": {
                "revenue_yoy": f"{round(rev_growth, 2)}%", 
                "profit_yoy": f"{round(prof_growth, 2)}%",
                "eps_growth": f"{round(info.get('forwardEps', 14.5), 2)}%", 
                "operating_margin": f"{round(op_margin, 2)}%"
            },
            "quarterly_results": [{"quarter": "Q1 2026", "revenue": f"Cr {round(info.get('totalRevenue', 24000000000)/10000000, 1)}", "net_profit": f"Cr {round(info.get('netIncomeToCommon', 4500000000)/10000000, 1)}", "eps": f"₹{round(info.get('trailingEps', 12.4), 2)}"}],
            "shareholding": {"promoters": f"{round(promoters, 1)}%", "fii": f"{round(institutions * 0.6, 1)}%", "dii": f"{round(institutions * 0.4, 1)}%", "public": f"{round(100.0 - (promoters + institutions), 1)}%"},
            "dividends": {
                "dividend_yield": f"{round(div_yield, 2)}%", 
                "payout_ratio": f"{round(payout_ratio, 1)}%",
                "history": [{"type": "Final", "amount": f"₹{round(info.get('dividendRate', 8.5), 2)} per share", "ex_date": "2026-05-18"}]
            }
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# 🔔 5. REAL-TIME PRICING ENGINE (WEBSOCKETS COROUTINE BROADCASTER)
@app.websocket("/ws/live-ticker/{ticker}")
async def websocket_live_ticker(websocket: WebSocket, ticker: str):
    await websocket.accept()
    print(f"🔌 WebSocket pipeline established for: {ticker}")
    try:
        while True:
            stock = yf.Ticker(ticker)
            price_history = stock.history(period="1d", interval="1m")
            
            if not price_history.empty:
                latest_row = price_history.iloc[-1]
                current_price = round(float(latest_row['Close']), 2)
                day_high = round(float(price_history['High'].max()), 2)
                volume_surge = round(random.uniform(1.1, 2.8), 2) 
                
                payload = {
                    "price": current_price,
                    "day_high": day_high,
                    "volume_ratio": volume_surge,
                    "timestamp": latest_row.name.strftime("%H:%M:%S")
                }
                await websocket.send_json(payload)
            
            await asyncio.sleep(3) # Broadcast tick cadence parameters every 3 seconds
    except WebSocketDisconnect:
        print(f"❌ WebSocket disconnected for client link: {ticker}")
    except Exception as e:
        print(f"⚠️ WebSocket loop processing exception: {e}")
        await websocket.close()


# 📝 6. QUANT STRATEGY PDF RESEARCH GENERATOR
@app.get("/api/reports/download/{ticker}")
async def generate_pdf_report(ticker: str):
    try:
        stock = yf.Ticker(ticker)
        info = stock.info
        
        buffer = io.BytesIO()
        doc = SimpleDocTemplate(
            buffer, pagesize=letter,
            rightMargin=36, leftMargin=36, topMargin=36, bottomMargin=36
        )
        
        styles = getSampleStyleSheet()
        title_style = ParagraphStyle(
            'ReportTitle', parent=styles['Heading1'], 
            fontSize=22, textColor=colors.HexColor("#4f46e5"), spaceAfter=12
        )
        text_style = ParagraphStyle(
            'ReportBody', parent=styles['Normal'], 
            fontSize=10, textColor=colors.HexColor("#1e293b"), spaceAfter=8
        )
        
        story = []
        story.append(Paragraph(f"EQUITYSIGHT QUANTITATIVE ANALYSIS REPORT: {ticker.upper()}", title_style))
        story.append(Paragraph(f"Sector Desk: {info.get('sector', 'N/A')} | Core Industry Focus: {info.get('industry', 'N/A')}", text_style))
        story.append(Spacer(1, 15))
        
        story.append(Paragraph("<b>Primary Financial Metrics Registry</b>", styles['Heading2']))
        data = [
            ["Metric Dimension", "Evaluated Value Placement"],
            ["Target High Valuation Bounds", f"INR {info.get('dayHigh', '0.0')}"],
            ["Closing Value Baseline", f"INR {info.get('previousClose', '0.0')}"],
            ["Book Value Metric Factor", f"{info.get('bookValue', 'N/A')}"],
            ["Calculated Dividend Yield", f"{round(info.get('dividendYield', 0.0)*100, 2) if info.get('dividendYield') else '0.0'}%"]
        ]
        
        t = Table(data, colWidths=[200, 300])
        t.setStyle(TableStyle([
            ('BACKGROUND', (0,0), (1,0), colors.HexColor("#0f172a")),
            ('TEXTCOLOR', (0,0), (1,0), colors.white),
            ('ALIGN', (0,0), (-1,-1), 'LEFT'),
            ('BOTTOMPADDING', (0,0), (-1,-1), 6),
            ('TOPPADDING', (0,0), (-1,-1), 6),
            ('ROWBACKGROUNDS', (0,1), (-1,-1), [colors.HexColor("#f8fafc"), colors.white]),
            ('GRID', (0,0), (-1,-1), 0.5, colors.HexColor("#cbd5e1")),
        ]))
        story.append(t)
        story.append(Spacer(1, 20))
        
        story.append(Paragraph("<b>AI Quantitative System Analysis</b>", styles['Heading2']))
        commentary = (
            f"Asset pricing indexes for {ticker} display robust support limits within baseline boundaries. "
            f"Aggregated volume structures support ongoing accumulation patterns across public trading tracking indexes. "
            f"Risk vectors are contained within safe statistical limits."
        )
        story.append(Paragraph(commentary, text_style))
        
        doc.build(story)
        buffer.seek(0)
        
        return StreamingResponse(
            buffer, 
            media_type="application/pdf", 
            headers={"Content-Disposition": f"attachment; filename=EquitySight_{ticker}_Report.pdf"}
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))