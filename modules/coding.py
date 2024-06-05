from itertools import permutations
import numpy as np
import modules.utility as utl


def canonic_components_permutations(components):
    if len(components) == 0:
        yield tuple()
        return
    parts = components[0].parts
    for head in permutations(filter(lambda x: x.parts == parts, components)):
        for tail in canonic_components_permutations(tuple(filter(lambda x: x.parts != parts, components))):
            yield *head, *tail


class Coding(object):
    def __init__(self, covers):
        self.covers = tuple(covers)

    def __len__(self):
        return len(self.covers)

    def __iter__(self):
        return iter(self.covers)

    def __lt__(self, other):
        for c_1, c_2 in zip(self.covers, other.covers):
            if c_1 == c_2:
                continue
            return c_1 < c_2

    def apply_translation(self, translation):
        return Coding(cover.apply_translation(translation) for cover in self.covers)

    def code_permutations(self):
        yield from canonic_components_permutations(self.covers)

    def is_sorted(self):
        # TODO: Check for sorting between Covers
        return all(c.is_sorted() for c in self.covers)

    def __repr__(self):
        return utl.seqence_to_str((c.parts for c in self.covers)) + " | " + ' - '.join(str(c) for c in self.covers)

    def to_save(self):
        return tuple(c.to_save() for c in self.covers)


class Cover(object):
    def __init__(self, cycles=None, weight=1):
        self.cycles, self.parts, self.weight = tuple(), tuple(), weight
        for c in cycles or tuple():
            self.add_cycle(c)
        # assert all(x >= y for x, y in zip(self.parts, self.parts[1:]))

    def __iter__(self):
        return zip(self.parts, self.cycles)

    def __eq__(self, other):
        return self.cycles == other.cycles

    def __lt__(self, other):
        return self.cycles < other.cycles

    def add_cycle(self, cycle):
        start = np.argmin(cycle)
        self.cycles += (cycle[start:] + cycle[:start],)
        self.parts += (len(cycle),)

    def sort(self):
        self.cycles = tuple(sorted(self.cycles, key=lambda x: (-len(x), *x)))

    def is_sorted(self):
        return all((-len(x), *x) < (-len(y), *y) for x, y in zip(self.cycles, self.cycles[1:]))

    def get_translations(self, index=0, start=0):
        if index == len(self.cycles):
            yield dict()
            return
        n = self.parts[index]
        for i in range(n):
            head_tr = {k: start + (i + j) % n for j, k in enumerate(self.cycles[index])}
            for tail_tr in self.get_translations(index + 1, start + n):
                tail_tr.update(head_tr)
                yield tail_tr

    def apply_translation(self, translation):
        new_cover = Cover(weight=self.weight)
        for cycle in self.cycles:
            new_cover.add_cycle(tuple(translation[i] for i in cycle))
        new_cover.sort()
        return new_cover

    def __repr__(self):
        return utl.seqence_to_str(self.cycles)

    def to_save(self):
        return self.cycles
