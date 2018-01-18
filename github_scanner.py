import asyncio
import aiohttp

from collections import MutableMapping
from functools import reduce
from utils import logger


API_URL = 'https://api.github.com'
ORG_OWNER = 'Github'
user = 'test_boo'
token = 'testtoken'



async def fetch(url=None, header=None):
    auth = aiohttp.BasicAuth(user, token)
    async with aiohttp.ClientSession() as session:
        if header is None:
            header = {
             'content-type': 'application/vnd.github.v3+json'
            }
        async with session.get(url, auth=auth, headers=header) as resp:
            assert resp.status in (200, 404)

        # if response.status != 200 and response.status != 304:
        #     raise Exception(url + ' - ' + body['message'])
        # return {
        #     'body': body,
        #     'headers': response.headers,
        #     'status': response.status
        # }
            return await resp.json()


async def get_topics(repos_list=None, ORG_OWNER=None):
    header = {'Accept': "application/vnd.github.mercy-preview+json"}
    all_topics = {}
    for repo_name in repos_list:
            endpoint = f'{API_URL}/repos/{ORG_OWNER}/{repo_name}/topics'
            all_topics[repo_name] = await fetch(endpoint, header)
    logger.info(f'Done scanning repositories for topics')
    return all_topics


async def get_repos(ORG_OWNER=None):
    repos = {}
    page = 1
    count = 0
    while True:
        try:
            url = f'{API_URL}/orgs/{ORG_OWNER}/repos?page={page}&per_page=100'
            json_repo = await fetch(url)
        except AssertionError:
            logger.error('Fetch page fail.')
            break
        if not json_repo:
            break
        else:
            page += 1
        for proj in json_repo:
            count += 1

            name = proj.get('name', '')

            if name in repos:
                # lng.upper()
                repos[name] += {'private':proj['private'], 'url':'/'.join(proj['git_url'].split('/')[2:]), 'pushed':proj['pushed_at']}
            else:
                repos[name] = {'private':proj['private'], 'url':'/'.join(proj['git_url'].split('/')[2:]), 'pushed':proj['pushed_at']}

    logger.info("Total repos: {}".format(count))
    return repos


async def check_file_path(repos, ORG_OWNER=None, path=None):
        response = {}
        for repo_name in repos:

                header = {'Accept': 'application/vnd.github.VERSION.object'}
                endpoint = f'{API_URL}/repos/{ORG_OWNER}/{repo_name}/contents/{path}'

                resp = await fetch(endpoint,  header)
                if 'message' in resp.keys():

                    response[repo_name] = {f'{path}': 'false'}
                else:
                    response[repo_name] = {f'{path}': 'true'}
        logger.info(f'Collected repos information for the {path}')
        return response


def merge_dictinaries(d1, d2):
    for k, v in d1.items():
        if k in d2:
            if all(isinstance(e, MutableMapping) for e in (v, d2[k])):
                d2[k] = merge_dictinaries(v, d2[k])
    d3 = d1.copy()
    d3.update(d2)
    return d3



def main():
    """Run the github crawler until all finished. """
    loop = asyncio.get_event_loop()
    repos_l = loop.run_until_complete(get_repos())
    all_repos = sorted(repos_l)
    topics = loop.run_until_complete(get_topics(sorted(all_repos)))
    readmes = loop.run_until_complete(check_file_path(all_repos, ORG_OWNER, 'README.md'))
    specs = loop.run_until_complete(check_file_path(all_repos, ORG_OWNER, 'service.def'))
    loop.close()
    return reduce(merge_dictinaries, (repos_l, topics, readmes, specs))


if __name__ == '__main__':

    main()