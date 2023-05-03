
from mongodb import get_client

__all__ = 'fill_rooms_pool',


def fill_rooms_pool() -> list[tuple[int, int]]:
    rooms_weight_map = {}

    db = get_client('remote_main')['bili_liveroom']

    # blive rooms 是不管直播状态、全天录制的房间
    _blive_rooms = db['settings'].find_one({'key': 'blive_rooms'})
    if not _blive_rooms:
        raise Exception("no blive_rooms")

    for room in _blive_rooms['value']:
        rooms_weight_map[tuple(room)] = 1000000

    # rooms_state 库提供正在直播的房间，加权排序后填充 rooms_pool
    ablive_rooms = db['rooms_state'].find(
        {},
        {
            '_id': 0,
            'uid': 1,
            'roomid': 1,
            'parent_name': 1,
            'watched_num': 1,
        }
    )
    for doc in ablive_rooms:
        rooms_weight_map.setdefault(
            (doc['uid'], doc['roomid']),
            # e.g: 电台区 105人看过 weight = 10 * (105 + 1) = 1060
            # e.g: 知识区 0人看过 weight = 5 * (0 + 1) = 5
            AREA_WEIGHT[doc['parent_name']] * (doc['watched_num'] + 1)
        )

    return [_[0] for _ in sorted(rooms_weight_map.items(), key=lambda x: x[1], reverse=True)]


AREA_WEIGHT = {
    '虚拟主播': 10,
    '电台': 10,
    '娱乐': 10,
    '生活': 10,
    '单机游戏': 1,
    '手游': 1,
    '网游': 1,
    '赛事': 1,
    '学习': 5,  # 学习区已经变成知识区
    '知识': 5,
}


if __name__ == '__main__':
    def test_fill_rooms_pool():
        rooms = fill_rooms_pool()
        print(len(rooms))
        print(rooms[:10])
        print(rooms[-10:])

    test_fill_rooms_pool()
