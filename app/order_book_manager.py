from typing import Dict, Optional


class OrderBook:
    def __init__(self, market_ticker: str) -> None:
        self._market_ticker = market_ticker
        self._yes_orders = {}
        self._no_orders = {}
        self._top_of_book_cache: Optional[Dict[str, int]] = None

    def update_from_snapshot(
        self,
        yes_orders: list[tuple[int, int]] | None = None,
        no_orders: list[tuple[int, int]] | None = None,
    ) -> None:
        yes_orders = yes_orders or {}
        no_orders = no_orders or {}

        # Populate with initial data
        for price, quantity in yes_orders:
            if quantity >= 0:  # Only store positive quantities
                self._yes_orders[price] = quantity

        for price, quantity in no_orders:
            if quantity >= 0:  # Only store positive quantities
                self._no_orders[price] = quantity

    def update_from_delta(self, price: int, delta: int, side: str) -> None:
        """Update order book with delta change"""
        # Invalidate cache when making changes
        self._top_of_book_cache = None

        if side == "yes":
            current_qty = self._yes_orders.get(price, 0)
            new_qty = current_qty + delta

            if new_qty <= 0:
                # Remove price level if quantity becomes zero or negative
                self._yes_orders.pop(price, None)
            else:
                self._yes_orders[price] = new_qty

        elif side == "no":
            current_qty = self._no_orders.get(price, 0)
            new_qty = current_qty + delta

            if new_qty <= 0:
                # Remove price level if quantity becomes zero or negative
                self._no_orders.pop(price, None)
            else:
                self._no_orders[price] = new_qty

    def top_of_book(self) -> Dict[str, Optional[int]]:
        """Get best bid and ask with caching"""
        if self._top_of_book_cache is not None:
            return self._top_of_book_cache

        # Find highest bid price (yes orders)
        if len(self._yes_orders.keys()) > 0:
            bid_price = max(self._yes_orders.keys()) 
            bid_quantity = (
                self._yes_orders.get(bid_price)
            )
        else:
            bid_price = None
            bid_quantity = None

        # Find lowest ask price (convert no orders to ask prices)
        # No order at price X means asking price of (100 - X)
        ask_price = None
        ask_quantity = None

        # Find the lowest ask price (highest no order price)
        if len(self._no_orders.keys()) > 0:
            highest_no_price = max(self._no_orders.keys())
            ask_price = 100 - highest_no_price
            ask_quantity = self._no_orders[highest_no_price]
        else:
            ask_price = None
            ask_quantity = None

        result = {
            "ticker": self._market_ticker,
            "bid_price": bid_price,
            "bid_quantity": bid_quantity,
            "ask_price": ask_price,
            "ask_quantity": ask_quantity,
        }

        # Cache the result
        self._top_of_book_cache = result
        return result

    def get_market_depth(self, levels: int = 10) -> Dict[str, list]:
        """Get market depth for top N levels"""
        # Sort yes orders by price (descending for bids)
        bid_levels = sorted(self._yes_orders.items(), reverse=True)[:levels]

        # Sort no orders by converted ask price (ascending for asks)
        # Convert no orders to ask format: (ask_price, quantity)
        ask_items = [(100 - price, qty) for price, qty in self._no_orders.items()]
        ask_levels = sorted(ask_items)[:levels]

        return {
            "bids": bid_levels,  # [(price, quantity), ...]
            "asks": ask_levels,  # [(price, quantity), ...]
        }


class OrderBookManager:
    """Manages multiple order books for different tickers"""

    def __init__(self):
        self._order_books: Dict[str, OrderBook] = {}

    def update_from_snapshot(
        self,
        market_ticker: str,
        yes_orders: list[tuple[int, int]],
        no_orders: list[tuple[int, int]],
    ) -> None:
        order_book = OrderBook(market_ticker)
        order_book.update_from_snapshot(yes_orders, no_orders)
        self._order_books[market_ticker] = order_book

    def update_from_delta(
        self, market_ticker: str, price: int, delta: int, side: str
    ) -> None:
        order_book = self._order_books.get(market_ticker)
        order_book.update_from_delta(price, delta, side)

    def get_order_book(self, market_ticker) -> OrderBook:
        if market_ticker in self._order_books:
            return self._order_books[market_ticker]
        else:
            return None

    def get_all_tickers(self) -> list[str]:
        return self._order_books.keys()
