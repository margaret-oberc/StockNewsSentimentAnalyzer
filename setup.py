from setuptools import setup, find_packages

setup(
    name="StockNewsSentimentAnalyzer",
    version="0.1",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    install_requires=[
        "pandas",
        "datetime",
        "pytz",
        "yfinance",
        "openai",
        "mysql-connector-python",
        "pydantic",
        "langchain",
        "pymysql",
        "scikit-learn",
        "statsmodels",
    ],
    entry_points={
        'console_scripts': [
            'sentiment_analyzer=main_sentiment_analyzer:main',
        ],
    },
)
