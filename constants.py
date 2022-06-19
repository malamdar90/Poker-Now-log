
patterns = [
    """-- starting hand #{}  (No Limit Texas Hold'em) (dealer: "{} @ {}") --""",
    "-- starting hand #{}  (No Limit Texas Hold'em) (dead button) --",
    "Player stacks: {}",
    '"{} @ {}" bets {} and go all in',
    '"{} @ {}" bets {}',
    '"{} @ {}" raises to {} and go all in',
    '"{} @ {}" raises to {}',
    '"{} @ {}" calls {} and go all in',
    '"{} @ {}" calls {}',
    '"{} @ {}" checks',
    '"{} @ {}" folds',
    '"{} @ {}" posts a {} of {} and go all in ',
    '"{} @ {}" posts a {} of {}',
    "Flop:  [{}, {}, {}]",
    "Turn: {}, {}, {} [{}]",
    "River: {}, {}, {}, {} [{}]",
    """"{} @ {}" collected {} from pot with {} (combination: {}, {}, {}, {}, {})""",
    '"{} @ {}" collected {} from pot',
    'Uncalled bet of {} returned to "{} @ {}"',
    '"{} @ {}" shows a {}, {}.',
]

action_round = {
    3: [1, 1],
    4: [1, 0],
    5: [2, 1],
    6: [2, 0],
    7: [3, 1],
    8: [3, 0],
    9: [4, 0],
    10: [5, 0],
    11: [None, 1],
    12: [None, 2],
}

rounds = {13: 1, 14: 2, 15: 3}
