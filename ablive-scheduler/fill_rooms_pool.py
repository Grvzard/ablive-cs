
from mongodb import get_client

__all__ = 'fill_rooms_pool',


class WeightedRoom:
    __slots__ = 'weight', 'room'

    def __init__(self, *, weight: int, room: tuple[int, int]):
        self.weight = weight
        self.room = room

    def __eq__(self, other):
        return hash(self) == hash(other)

    def __hash__(self):
        return hash(self.room)


def fill_rooms_pool() -> list[tuple[int, int]]:
    weighted_rooms = set()

    db = get_client('remote_main')['bili_liveroom']

    # blive rooms 是不管直播状态、全天录制的房间
    blive_rooms = db['settings'].find_one({'key': 'blive_rooms'})['value']
    blive_rooms = {tuple(room) for room in blive_rooms}
    for room in blive_rooms:
        weighted_room = WeightedRoom(
            weight = 100000000,
            room = room,
        )
        weighted_rooms.add(weighted_room)

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
        weighted_room = WeightedRoom(
            # e.g: 电台区 105人看过 weight = 10 * (105 + 1) = 1060
            # e.g: 知识区 0人看过 weight = 5 * (0 + 1) = 5
            weight = AREA_WEIGHT[doc['parent_name']] * (doc['watched_num'] + 1),
            room = (doc['uid'], doc['roomid']),
        )
        weighted_rooms.add(weighted_room)

    weighted_rooms = list(weighted_rooms)
    weighted_rooms.sort(reverse=True, key=lambda x: x.weight)

    rooms_pool = [x.room for x in weighted_rooms]

    return rooms_pool


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
    rooms = fill_rooms_pool()
    print(len(rooms))
    print(rooms[30:])
    print(rooms[:30])
