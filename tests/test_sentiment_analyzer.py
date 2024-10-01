import unittest
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))
from stock_news_sentiment_analyzer import get_sentiment_analysis

class TestSentimentAnalyzer(unittest.TestCase):

    def test_sentiment_analysis(self):
        stock = "BCE"
        article = "BCE stock has shown tremendous growth in recent times, making it a good buy."

        result = get_sentiment_analysis(stock, article)
        self.assertIsNotNone(result)
        self.assertIn(result.score, [0, 1])

if __name__ == '__main__':
    unittest.main()
