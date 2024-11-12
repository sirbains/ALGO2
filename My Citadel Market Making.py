import asyncio
import aiohttp
import numpy as np
import pandas as pd
import logging
from datetime import datetime

# Constants and API setup
API_KEY = {'X-API-key': 'MW0YJ28H'}
BASE_URL = 'http://localhost:9999/v1/'
MAX_POSITION_LIMIT = 24000
INITIAL_ORDER_SIZE = 2100  # Lower initial order size for better control
BASE_SPREAD_THRESHOLD = 0.03  # Higher threshold to ensure profitability
ORDER_DELAY_BASE = 0.9
REBATE_PER_SHARE = 0.015
FEE_PER_SHARE = 0.01
OVER_LIMIT_FINE = 500
POSITION_BUFFER = 0.9  # Buffer for position cap, stop trading at 90% of max

# Configure detailed logging for performance monitoring
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

# Fetch data from the RIT API
async def fetch_data(session, endpoint, params=None):
    async with session.get(BASE_URL + endpoint, headers=API_KEY, params=params) as response:
        return await response.json()

async def get_market_data(session):
    return await fetch_data(session, 'securities/book', {'ticker': 'ALGO'})

async def get_position(session):
    data = await fetch_data(session, 'securities', {'ticker': 'ALGO'})
    return data[0]['position']

async def get_tick(session):
    case_data = await fetch_data(session, 'case')
    return case_data['tick']

# Calculate adaptive spread threshold based on real-time conditions
def calculate_spread_threshold():
    return BASE_SPREAD_THRESHOLD

# Calculate expected profitability based on spread, fees, and potential fines
def is_profitable(spread, bid_size, ask_size, over_limit=False):
    expected_rebate = (bid_size + ask_size) * REBATE_PER_SHARE
    expected_fee = (bid_size + ask_size) * FEE_PER_SHARE
    if over_limit:
        expected_fee += OVER_LIMIT_FINE
    return spread >= BASE_SPREAD_THRESHOLD + (expected_fee - expected_rebate)

# Place passive limit orders with controlled size and spread threshold
async def place_passive_orders(session, bid_price, ask_price, bid_size, ask_size, position):
    # Check if position exceeds 90% of the limit to avoid fines
    over_limit = abs(position) + max(bid_size, ask_size) > MAX_POSITION_LIMIT * POSITION_BUFFER
    spread = abs(ask_price - bid_price)

    if is_profitable(spread, bid_size, ask_size, over_limit):
        # Place buy order if within position limits
        if position < MAX_POSITION_LIMIT * POSITION_BUFFER:
            bid_payload = {'ticker': 'ALGO', 'type': 'LIMIT', 'quantity': bid_size, 'price': bid_price, 'action': 'BUY'}
            bid_response = await session.post(BASE_URL + 'orders', params=bid_payload, headers=API_KEY)
            if bid_response.status == 200:
                logging.info(f"Placed bid at {bid_price} for {bid_size} shares.")
            else:
                logging.error(f"Failed to place bid: {await bid_response.text()}")
        
        # Place sell order if within position limits
        if position > -MAX_POSITION_LIMIT * POSITION_BUFFER:
            ask_payload = {'ticker': 'ALGO', 'type': 'LIMIT', 'quantity': ask_size, 'price': ask_price, 'action': 'SELL'}
            ask_response = await session.post(BASE_URL + 'orders', params=ask_payload, headers=API_KEY)
            if ask_response.status == 200:
                logging.info(f"Placed ask at {ask_price} for {ask_size} shares.")
            else:
                logging.error(f"Failed to place ask: {await ask_response.text()}")

# Main trading loop with conservative starting strategy and adaptive delay
async def main():
    async with aiohttp.ClientSession() as session:
        while True:
            tick = await get_tick(session)
            order_book = await get_market_data(session)
            position = await get_position(session)

            # Calculate mid prices and spread
            weighted_bid = order_book['bids'][0]['price']
            weighted_ask = order_book['asks'][0]['price']
            spread = weighted_ask - weighted_bid
            
            # Adjust order sizes based on market conditions
            bid_size = ask_size = INITIAL_ORDER_SIZE

            # Place orders with enhanced controls and profitability check
            await place_passive_orders(session, weighted_bid, weighted_ask, bid_size, ask_size, position)

            # Apply delay to control trade frequency
            await asyncio.sleep(ORDER_DELAY_BASE)

# Run the algorithm
asyncio.run(main())
