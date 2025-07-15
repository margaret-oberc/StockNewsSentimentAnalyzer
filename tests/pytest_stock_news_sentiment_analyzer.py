import pytest
import pandas as pd
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime
import pytz

import stock_news_sentiment_analyzer as analyzer


def test_utc_to_est_valid():
    utc_time = datetime(2023, 1, 1, 12, 0, 0, tzinfo=pytz.utc)
    result = analyzer.utc_to_est(utc_time)
    assert result == "2023-01-01 07:00:00"


def test_utc_to_est_invalid():
    result = analyzer.utc_to_est("invalid")
    assert result is None


@patch('stock_news_sentiment_analyzer.feedparser.parse')
def test_get_news_success(mock_parse):
    mock_parse.return_value = Mock(bozo=False, entries=[
        Mock(id='1', title='News Title', link='http://link.com', published='Mon, 01 Jan 2023 12:00:00 +0000', description='Sample description')
    ])

    df = analyzer.get_news('AAPL')
    assert isinstance(df, pd.DataFrame)
    assert not df.empty
    assert 'uuid' in df.columns


@patch('stock_news_sentiment_analyzer.feedparser.parse')
def test_get_news_failure(mock_parse):
    mock_parse.return_value = Mock(bozo=True)
    assert analyzer.get_news('AAPL') is None


def test_article_exists_found():
    mock_conn = Mock()
    mock_cursor = MagicMock()
    mock_cursor.fetchone.return_value = (1,)
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

    assert analyzer.article_exists(mock_conn, 'some-uuid') is True


def test_article_exists_not_found():
    mock_conn = Mock()
    mock_cursor = MagicMock()
    mock_cursor.fetchone.return_value = None
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

    assert analyzer.article_exists(mock_conn, 'some-uuid') is False


@patch('stock_news_sentiment_analyzer.td.get_trading_date', return_value='2023-01-01')
def test_insert_ynews(mock_get_trading_date):
    mock_conn = Mock()
    mock_cursor = Mock()
    mock_conn.cursor.return_value = mock_cursor

    row = pd.Series({
        'uuid': '1234',
        'title': 'Test Title',
        'description': 'Test Description',
        'link': 'http://test.com',
        'est_time': datetime(2023, 1, 1),
        'type': 'story',
        'score': 1,
        'comment': 'Positive news'
    })

    analyzer.insert_ynews(row, mock_conn, 'AAPL')
    assert mock_cursor.execute.called
    assert mock_conn.commit.called


@patch('stock_news_sentiment_analyzer.client.beta.chat.completions.parse')
def test_get_sentiment_analysis_success(mock_parse):
    mock_response = Mock()
    mock_response.choices = [Mock(message=Mock(parsed=Mock(score=1, type='story', comment='Positive')))]
    mock_parse.return_value = mock_response

    result = analyzer.get_sentiment_analysis("AAPL", "Good earnings", "Company beats estimates")
    assert result.score == 1
    assert result.type == "story"
    assert result.comment == "Positive"


@patch('stock_news_sentiment_analyzer.client.beta.chat.completions.parse', side_effect=Exception("API error"))
def test_get_sentiment_analysis_failure(mock_parse):
    result = analyzer.get_sentiment_analysis("AAPL", "Title", "Body")
    assert result is None
