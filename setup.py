import setuptools

setuptools.setup(
    name="twesearch",
    version="0.0.1",
    author="Chris Dehghanpoor",
    author_email="c@chriskd.me",
    url="https://github.com/chriskd/twesearch",
    packages=setuptools.find_packages(),
    python_requires='>=3.6',
    install_requires=["searchtweets-v2", "tweepy", "Click"],
    scripts=[
            'bin/crawler.py',
            'bin/follower_utils.py',
            'bin/get_timeline.py',
            'bin/add_query.py'
            ]
)
