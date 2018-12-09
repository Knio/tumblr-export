import json
import logging
import os
import queue
import re
import string
import threading

import requests


API_KEY = '8YUsKJvcJxo2MDwmWMDiXZGuMuIbeCwuQGP5ZHSEA4jBJPMnJT'

log = logging.getLogger('download')

SAFE = set(string.ascii_lowercase + string.digits + '.')
TRANS = {i: chr(i) if chr(i) in SAFE else '_' for i in range(256)}
def sanitize(s):
    return s.lower().translate(TRANS)


class BlogArchive(object):
    def __init__(self, root):
        self.root = root

    def path(self, *parts):
        return os.path.join(self.root, *parts)

    def save(self, content, path):
        os.makedirs(os.path.dirname(path), exist_ok=True)
        open(path, 'wb').write(content)

    def download(self, url, path):
        if os.path.isfile(path):
            return
        log.info(url)
        try:
            r = requests.get(url)
            r.raise_for_status()
        except requests.exceptions.HTTPError as e:
            if e.response.status_code in (403, 404):
                log.info('%r', e)
                return
            raise
        self.save(r.content, path)

    def backup_og(self, post):
        self.backup_post(post)

    def backup_like(self, post):
        blog = sanitize(post['blog']['name'])
        self.backup_post(post, 'likes', blog)

    def backup_post(self, post, *parts):
        path = self.path(*parts, 'posts', '{[id]:d}.json'.format(post))
        self.save(json.dumps(post, indent=2, sort_keys=True).encode('utf-8'), path)

        if 'photos' in post:
            for photo in post['photos']:
                url = photo['original_size']['url']
                path = self.path(*parts, 'photos', os.path.basename(url))
                self.download(url, path)

        if post['type'] in ('photo', 'link'):
            # handled elsewhere or no content
            pass

        elif post['type'] == 'video':
            if post['video_type'] != 'tumblr':
                log.warning('Cannot download video type %r for %s',
                        post['video_type'], post['short_url'])
                return
            try:
                url = post['video_url']
            except KeyError:
                log.info(json.dumps(post, sort_keys=True, indent=2))
                raise
            path = self.path(*parts, 'videos', os.path.basename(url))
            self.download(url, path)

        elif post['type'] in ('text', 'answer'):
            if post['type'] == 'text':
                body = post['body']
            elif post['type'] == 'answer':
                body = post['answer']
            img_re = r'<img src="([^"]+)"'
            for url in re.findall(img_re, body):
                path = self.path(*parts, 'photos', os.path.basename(url))
                self.download(url, path)

        else:
            log.error('Unknown post! %s', json.dumps(post, indent=2, sort_keys=False))


class Blog(object):
    def __init__(self, name):
        self.name = name

    def get_likes(self):
        return self.get_stream('likes')

    def get_posts(self):
        return self.get_stream('posts')

    def get_stream(self, stream):
        params = {
            'api_key': API_KEY,
            'limit': 50,
            'reblog_info': 'true',
        }
        i = 0
        while True:
            r = requests.get('https://api.tumblr.com/v2/blog/{}/{}'.format(
                    self.name, stream), params=params)
            r.raise_for_status()
            j = r.json()
            # log.info(json.dumps(j, indent=2, sort_keys=True))
            assert j['meta']['msg'] == 'OK'
            assert j['meta']['status'] == 200
            if stream == 'posts':
                posts = j['response']['posts']
            elif stream == 'likes':
                posts = j['response']['liked_posts']
            else:
                raise ValueError
            if not posts:
                break
            for post in posts:
                yield post
                i += 1
            params.update(j['response']['_links']['next']['query_params'])
            if stream == 'likes':
                total = j['response']['liked_count']
            elif stream == 'posts':
                total = j['response']['total_posts']
            else:
                total = 0
            log.info('Loaded %d/%d %s', i, total, stream)


def work_queue(func, work, num_threads=1):
    # https://docs.python.org/3/library/queue.html#queue.Queue.join
    q = queue.Queue(num_threads)

    def worker():
        while True:
            x = q.get()
            if x is None:
                break
            try:
                func(x)
            except:
                logging.exception('Error')
            q.task_done()

    threads = []
    for i in range(num_threads):
        t = threading.Thread(target=worker)
        t.daemon = True
        t.start()
        threads.append(t)

    for x in work:
        q.put(x)

    q.join()

    for i in range(num_threads):
        q.put(None)

    for t in threads:
        t.join()


def main():
    logging.basicConfig(
        level=logging.INFO,
        format='%(levelname)s:%(filename)s:%(funcName)s:%(lineno)d:%(message)s')
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('blog',
        help='Tumblr blog full name, i.e. "foo.tumblr.com"')
    args = parser.parse_args()
    b = Blog(args.blog)
    a = BlogArchive(sanitize(args.blog))

    work_queue(a.backup_og, b.get_posts(), 10)
    work_queue(a.backup_like, b.get_likes(), 10)


if __name__ == '__main__':
    main()
