import json
import pprint
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from decimal import Decimal
from itertools import count
from pathlib import Path

import click
import requests
import appdirs
from eth_abi import encode_single
from eth_utils import function_signature_to_4byte_selector, decode_hex, encode_hex
from web3 import Web3
from web3.middleware import construct_sign_and_send_raw_middleware
from web3.exceptions import NoABIFunctionsFound, MismatchedABI

WEB3_TIMEOUT = 120

# WEB3_PROVIDER_URI = "wss://kovan.infura.io/ws"
# WEB3_PROVIDER_URI = "https://parity1.kovan.makerfoundation.com:8545/"
# WEB3_PROVIDER_URI = "https://parity1.mainnet.makerfoundation.com:8545/"
# WEB3_PROVIDER_URI = "https://parity.expotrading.com/"
WEB3_PROVIDER_URI = "https://parity0.mainnet.makerfoundation.com:8545/"

# build web3 provider
w3 = Web3(Web3.HTTPProvider(
    WEB3_PROVIDER_URI,
    request_kwargs={
        'timeout': WEB3_TIMEOUT
    }
))

CHIEF_ADDRESS_PROD = w3.toChecksumAddress(
    '0x9eF05f7F6deB616fd37aC3c959a2dDD25A54E4F5'
)
CHIEF_ADDRESS = w3.toChecksumAddress(
    '0xbBFFC76e94B34F72D96D054b31f6424249c1337d'
)

# kovan
# CHIEF_BLOCK = 6591861
# CAST_SPELL = False
# LIFT_PROPOSAL = False

# mainnet
CHIEF_ADDRESS = CHIEF_ADDRESS_PROD
CHIEF_BLOCK = 7705361
CAST_SPELL = True
LIFT_PROPOSAL = True

# if setting CAST_SPELL True, also set LIFT_PROPOSAL True
if CAST_SPELL:
    LIFT_PROPOSAL = True

# DO NOT COMMIT OR EXPOSE THIS
PRIVATE_KEY = 'DEADBEEF...'
account = w3.eth.account.privateKeyToAccount(PRIVATE_KEY)
w3.middleware_onion.add(construct_sign_and_send_raw_middleware(account))
ETH_FROM = w3.toChecksumAddress(account.address)

pool = ThreadPoolExecutor(10)
cache = Path(appdirs.user_cache_dir('chief'))
cache.mkdir(exist_ok=True)

@dataclass
class Voter:
    '''Anyone can vote for multiple proposals (yays).'''
    yays: list = field(default_factory=list)
    weight: Decimal = Decimal()

def to_32byte_hex(val):
    return w3.toHex(w3.toBytes(val).rjust(32, b'\0'))

def get_contract(address):
    '''Get contract interface and cache it.'''
    f = cache / f'{address}.json'
    # f = cache / f'{CHIEF_ADDRESS_PROD}.json'
    if not f.exists():
        # cache the response
        abi = get_contract_abi(address)
        f.write_text(json.dumps(abi))
    abi = json.loads(f.read_text())
    return w3.eth.contract(address, abi=abi)


def get_contract_abi(address):
    '''Get contract interface from Etherscan.'''
    resp = requests.get('http://api.etherscan.io/api', params={
        'module': 'contract',
        'action': 'getabi',
        'format': 'raw',
        'address': address,
    })
    try:
        return resp.json()
    except json.JSONDecodeError:
        return


def get_slates(chief):
    '''Get unique sets of proposals.'''
    etches = chief.events.Etch().createFilter(fromBlock=CHIEF_BLOCK).get_all_entries()
    slates = {encode_hex(etch['args']['slate']) for etch in etches}
    #pp = pprint.PrettyPrinter(indent=4)
    #pp.pprint(slates)
    return slates


def slates_to_yays(chief, slates):
    '''Concurrently get corresponding votes for slates.'''
    yays = {slate: pool.submit(slate_to_addresses, chief, slate) for slate in slates}
    return {slate: yays[slate].result() for slate in slates}


def slate_to_addresses(chief, slate):
    '''Get all proposals a slate votes for.'''
    addresses = []
    for i in count():
        try:
            addresses.append(chief.functions.slates(slate, i).call())
        except ValueError:
            break
    return addresses


def func_topic(func):
    ''' Convert function signature to ds-note log topic. '''
    return encode_hex(encode_single('bytes32', function_signature_to_4byte_selector(func)))


def get_notes(chief):
    '''Get yays and slate votes.'''
    return w3.eth.getLogs({
        'address': chief.address,
        'topics': [
            [func_topic('vote(address[])'), func_topic('vote(bytes32)')]
        ],
        'fromBlock': CHIEF_BLOCK,
    })


def notes_to_voters(chief, notes, slates_yays):
    '''Recover the most recent votes for each user and their deposit.'''
    voters = defaultdict(Voter)
    for note in notes:
        data = decode_hex(note['data'])[96:]
        try:
            func, args = chief.decode_function_input(data)
        except:
            continue
        sender = w3.toChecksumAddress(note['topics'][1][12:])
        v = voters[sender]
        v.yays = slates_yays.get(encode_hex(args['slate']), []) if 'slate' in args else args['yays']
    deposits = {v: pool.submit(voter_deposit, chief, v) for v in voters}
    for v in voters:
        voters[v].weight = deposits[v].result()
    return voters


def voter_deposit(chief, address):
    '''Get MKR deposit of a user in the governance contract.'''
    return w3.fromWei(chief.functions.deposits(address).call(), 'ether')


def voters_to_results(voters):
    '''Tally the votes.'''
    proposals = Counter()
    for addr in voters:
        for yay in voters[addr].yays:
            proposals[yay] += voters[addr].weight
    return proposals.most_common()


def votes_for_proposal(proposal, voters):
    '''Find all votes for a proposal.'''
    votes = Counter()
    for addr in voters:
        if proposal in voters[addr].yays and voters[addr].weight > 0:
            votes[addr] = voters[addr].weight
    return votes.most_common()


def decode_spell(address):
    '''Decode ds-spell called against mom contract.'''
    spell = get_contract(address)
    proposal = {
        'name': 'None',
        'args': {},
        'desc': None,
        'cast': spell.functions.done().call()
    }
    try:
        whom = spell.functions.whom().call()
        mom = get_contract(whom)
        func, args = mom.decode_function_input(spell.functions.data().call())
        if func.fn_name == 'setFee':
            rate = Decimal(args['ray']) / 10 ** 27
            percent = rate ** (60 * 60 * 24 * 365) * 100 - 100
            proposal['name'] = func.fn_name
            proposal['args'] = args
            proposal['desc'] = f'{percent:.2f}%'
    except (ValueError, NoABIFunctionsFound, MismatchedABI):
        pass
    return proposal


def get_spells(addresses):
    '''Try to decode all spells.'''
    spells = {}
    for spell in addresses:
        try:
            spells[spell] = decode_spell(spell)
        except (ValueError, NoABIFunctionsFound, MismatchedABI):
            pass
    return spells


def output_text(chief, voters, results, spells, hat):
    '''Output results as text.'''
    max_votes = 0
    for i, (proposal, votes) in enumerate(results, 1):
        if votes > max_votes:
            max_votes = votes

        if proposal == hat:
            click.secho(f'{i}. {proposal} {votes}', fg='green', bold=True)
        elif proposal in spells:
            if spells[proposal]['cast'] == False:
                click.secho(f'{i}. {proposal} {votes}', fg='cyan', bold=True)
            else:
                click.secho(f'{i}. {proposal} {votes}', fg='yellow', bold=True)
        else:
            click.secho(f'{i}. {proposal} {votes}', fg='red', bold=True)

        if proposal in spells:
            s = spells[proposal]

            if votes == max_votes:
                if proposal != hat:
                    lift_proposal(chief, proposal)

                if proposal == hat and s['cast'] == False:
                    cast_spell(chief, proposal)

            # describe spell if we can
            if ('name' in s):
                c = 'can cast'
                if s['cast'] == True:
                    c = 'already cast'

                if s['name'] != 'None':
                    click.secho(
                        f"spell({c}): {s['name']} {s['desc']} {s['args']}",
                        fg='cyan' if s['cast'] == False else 'yellow'
                    )
                else:
                    click.secho(
                        f"spell({c})",
                        fg='cyan' if s['cast'] == False else 'yellow'
                    )

            for voter, weight in votes_for_proposal(proposal, voters):
                click.secho(f'  {voter} {weight}')

            print()


def output_json(voters, results, spells, hat):
    '''Output results as json. Use --json option for that.'''
    data = {'hat': hat, 'proposals': {}}
    for proposal, votes in results:
        data['proposals'][proposal] = {
            'total': votes,
            'voters': dict(votes_for_proposal(proposal, voters)),
            'spell': spells.get(proposal),
        }
    click.secho(json.dumps(data, indent=2, default=str))

def lift_proposal(chief, proposal):
    '''Lift proposal to hat'''
    if (LIFT_PROPOSAL):
        click.secho(f"chief.lift({proposal})", fg='magenta')
        tx_hash = chief.functions.lift(proposal).transact({
            'from': ETH_FROM
        })
        tx_receipt = w3.eth.waitForTransactionReceipt(tx_hash)
        click.secho(f"{proposal} lifted!", fg='magenta')

def cast_spell(chief, proposal):
    '''Cast spell to execute proposal'''
    if (CAST_SPELL):
        click.secho(f"spell.cast()", fg='magenta')
        spell = get_contract(proposal)
        tx_hash = spell.functions.cast().transact({
            'from': ETH_FROM
        })
        tx_receipt = w3.eth.waitForTransactionReceipt(tx_hash)
        click.secho(f"{proposal} cast!", fg='magenta')

@click.command()
@click.option('--json', is_flag=True)
def main(json):
    chief = get_contract(CHIEF_ADDRESS)
    print('got chief')
    slates = get_slates(chief)
    print('got slates')
    slates_yays = slates_to_yays(chief, slates)
    print('got yays')

    notes = get_notes(chief)
    print('got notes')
    voters = notes_to_voters(chief, notes, slates_yays)
    print('got voters')

    results = voters_to_results(voters)
    print('got results')
    spells = get_spells([proposal for proposal, votes in results])
    print('got spells')
    hat = chief.functions.hat().call()
    print('got hat')

    if json:
        output_json(voters, results, spells, hat)
    else:
        output_text(chief, voters, results, spells, hat)


if __name__ == '__main__':
    main()
