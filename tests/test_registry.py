from core.registry import HandlerRegistry


def test_register_and_get():
    r = HandlerRegistry()

    @r.register("test.job")
    def handler(job):
        return {}

    assert r.get("test.job") is handler


def test_unknown_type_returns_none():
    r = HandlerRegistry()
    assert r.get("nonexistent") is None


def test_register_overwrites():
    r = HandlerRegistry()

    @r.register("test.job")
    def h1(job):
        return {"v": 1}

    @r.register("test.job")
    def h2(job):
        return {"v": 2}

    assert r.get("test.job") is h2


def test_known_types():
    r = HandlerRegistry()

    @r.register("a")
    def h1(job):
        pass

    @r.register("b")
    def h2(job):
        pass

    assert set(r.known_types()) == {"a", "b"}
