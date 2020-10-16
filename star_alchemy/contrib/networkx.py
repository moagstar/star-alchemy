import networkx as nx

from star_alchemy import StarSchema


def to_nx(star_schema: StarSchema):
    """

    :param star_schema:

    :return:
    """
    G = nx.DiGraph()

    for s in star_schema:
        G.add_node(s.name, name=str(s.name))
        if s.parent is not None:
            G.add_edge(s.parent.name, s.name)

    return G
