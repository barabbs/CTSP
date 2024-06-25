from itertools import permutations
import numpy as np
import modules.utility as utl


def cover_components_reordering(components, parts):
    if len(components) == 0:
        yield tuple()
        return
    part = parts[0]
    for _, head in permutations(filter(lambda x: x[0] == part, zip(parts, components))):
        for _, tail in cover_components_reordering(tuple(filter(lambda x: x[0] != part, zip(parts, components)))):
            yield *head, *tail


class Coding(object):
    def __init__(self, covers):
        self.covers = covers

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
        return Coding(tuple(cover.apply_translation(translation) for cover in self.covers))

    def code_reordering(self, components=None):
        components = components if components is not None else self.covers
        if len(components) == 0:
            yield tuple()
            return
        parts = components[0].parts
        for head in permutations(filter(lambda x: x.parts == parts, components)):
            for tail in self.code_reordering(tuple(filter(lambda x: x.parts != parts, components))):
                yield (*head, *tail)

    def is_sorted(self):
        # TODO: Check for sorting between Covers
        return all(c.is_sorted() for c in self.covers)

    def is_canon(self):
        for code_reorder in self.code_reordering(self.covers):
            coding_reorder = Coding(code_reorder)
            base_cover = next(iter(coding_reorder))
            for cover_reorder in base_cover.cover_reordering():
                for trans in Cover(cover_reorder).get_translations():
                    if coding_reorder.apply_translation(trans) < self:
                        return False
        return True

    def get_translations(self):
        translations = list()
        for code_reorder in self.code_reordering(self.covers):
            # order = sum(code_reorder[0].cycles, start=tuple())
            coding_reorder = Coding(code_reorder)
            base_cover = next(iter(Coding(coding_reorder)))
            for cover_reorder in base_cover.cover_reordering():
                for trans in Cover(cover_reorder).get_translations():
                    # print("\t".join(f"<{k}> {v}" for k, v in sorted(trans.items(), key=lambda x: order.index(x[0]))))
                    translations.append(coding_reorder.apply_translation(trans))
        return translations

    def __invert__(self):
        return Coding(covers=self.covers[::-1])

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

    def cover_reordering(self, components=None):
        components = components if components is not None else self.cycles
        if len(components) == 0:
            yield tuple()
            return
        part = len(components[0])
        for head in permutations(tuple(filter(lambda x: len(x) == part, components))):
            for tail in self.cover_reordering(tuple(filter(lambda x: len(x) != part, components))):
                yield (*head, *tail)


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


if __name__ == '__main__':
    coding = (((0, 1, 2, 3), (4, 5)), ((0, 2, 3, 5), (1, 4)))  # ((0, 4), (1, 2), (3, 5)))
    coding = Coding(tuple(Cover(cycles=c) for c in coding))
    print(coding)
    print(~coding)
    print("-" * 32 + "\n" + "\n".join(str(s) for s in coding.get_translations()))
