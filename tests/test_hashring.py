from dsfs.hashring import HashRing


def test_hashring():
    hr = HashRing()
    hr.add(0, 0)
    hr.add(2, 2)
    hr.add(4, 4)

    assert hr.get(0) == 2
    assert hr.get(1) == 2
    assert hr.get(2) == 4
    assert hr.get(3) == 4
    assert hr.get(4) == 0
    assert hr.get(5) == 0
