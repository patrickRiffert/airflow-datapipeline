from airflow.hooks.http_hook import HttpHook
import requests
import json

class TwitterHook(HttpHook):

    def __init__(self, query, conn_id=None, start_time=None, end_time=None):
        self.query = query
        self.conn_id = conn_id or 'twitter_default'
        self.start_time = start_time
        self.end_time = end_time
        super().__init__(http_conn_id=self.conn_id)

    def create_url(self):
        query = self.query
        tweet_fields = "tweet.fields=author_id,created_at,lang,text"
        user_fields = "expansions=author_id&user.fields=id,name,username,created_at"
        url = "{}/2/tweets/search/recent?query={}&{}&{}&max_results=50".format(
            self.base_url, query, tweet_fields, user_fields)
        return url

    def connect_to_endpoint(self, url, session):
        response = requests.Request("GET", url)
        prep = session.prepare_request(response)
        self.log.info(f'URL Requisitada: {url}')
        return self.run_and_check(session, prep, {}).json()

    def run(self):
        session = self.get_conn()
        url = self.create_url()
        return self.connect_to_endpoint(url, session)


if __name__ == '__main__':
    response_content = TwitterHook('Boticário').run()
    print(json.dumps(response_content, indent=4, sort_keys=True))