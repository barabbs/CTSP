import matplotlib.pyplot as plt
import pynauty
from modules.combinatorics import *
from modules.coding import Coding, Cover
from modules import optimization_models as opt_mdl
import modules.utility as utl
from modules import graphics
from modules import var
import time



PROPERTIES = (
    'subt',  # graph satisfies subtour elimination constraints
    'extr',  # graph is extremal solution of SEP
    'canon',  # graph code is canonical
    # 'subt_old',  # graph satisfies subtour elimination (LEGACY)
    # 'extr_old',  # graph is extremal solution of SEP   (LEGACY)
    # 'sort',  # graph code is sorted                    (LEGACY)
    # 'plan'  # graph is planar                          (LEGACY)
)

class ClovenGraph(object):

    def __init__(self, n, coding=None, components=None):
        super().__init__()
        self._coding = coding or Coding(Cover(c) for c in components)
        self.n, self.k = n, len(self._coding)
        self.graph = utl.get_graph_from_code(self._coding)
        self.certificate, self.GAP = None, None
        self.properties, self.timings = dict((p, None) for p in PROPERTIES), dict()

    @staticmethod
    def timing(time_type):
        def timing_decorator(function):
            def wrapper(self, *args, **kwargs):
                start = time.process_time_ns()
                result = function(self, *args, **kwargs)
                self.timings[time_type] = time.process_time_ns() - start
                return result

            return wrapper

        return timing_decorator

    # PRIVATE UTILITIES

    def _apply_translation(self, translation):
        return tuple(cover.apply_translation(translation) for cover in self._coding)

    def _edges_in_subgraph(self, s):
        return self.graph.subgraph(s).size()

    # CALCULATIONS

    @timing('calc:GAP')
    def calc_GAP(self):
        self.GAP = opt_mdl.GAP_MODEL.solve_instance(self.n, utl.get_adjacency_matrix(self.n, self.k, self.graph))

    @timing('calc:certificate')
    def calc_certificate(self):
        nauty = utl.get_nauty_graph(self.n, self.k, self.graph)
        self.certificate = pynauty.certificate(nauty)

    @timing('calc:canonic_code')
    def calc_canonic_code(self):
        code = self._coding
        # print(f"\t\tBEGIN {code}")
        for j, comp in enumerate(self._coding.code_permutations()):
            # print(f"\t\t\tCOMP {comp}")
            for i, trans in enumerate(comp[0].get_translations()):
                new_code = self._coding.apply_translation(trans)
                # print(f"\t\t\t\t{new_code}\t\t{trans}")
                code = min(code, new_code)
        return code

    # PROPERTIES CHECK

    @timing('prop:subt_extr')
    def check_subt_extr(self):
        self.properties['subt'], self.properties['extr'] = opt_mdl.SEP_MODEL.check_subt_extr(self.n,
                                                                                             utl.get_adjacency_matrix(
                                                                                                 self.n, self.k,
                                                                                                 self.graph))

    @timing('prop:subt_extr_old')
    def check_subt_extr_old(self):  # TODO: CHECK FOR CONSISTENCY AND SPEED
        # TODO: Improve algorithm taking components into account
        check_for_extr = self.properties['extr_old'] is None
        for k in range(2, self.n - 1):
            max_size = 2 * (k - 1)
            for s in combinations(self.graph.nodes, k):
                size = self._edges_in_subgraph(s)
                if size > max_size:
                    self.properties['subt_old'] = False
                    # self.properties['SEP_counterexample'] = s
                    return
                elif check_for_extr and size == max_size:
                    self.properties['extr_old'] = True
                    # self.properties['extr_active_bound'] = s
                    check_for_extr = False
        if check_for_extr:
            self.properties['extr_old'] = False
        self.properties['subt_old'] = True

    @timing('prop:plan')
    def check_planarity(self):  # TODO: LEGACY ---> REMOVE
        self.properties['plan'] = nx.is_planar(self.graph)

    @timing('prop:sort')
    def check_sorted(self):  # TODO: LEGACY ---> REMOVE
        self.properties['sort'] = self._coding.is_sorted()

    @timing('prop:canon')
    def check_canon(self):
        code = self._coding
        for j, comp in enumerate(self._coding.code_permutations()):
            for i, trans in enumerate(comp[0].get_translations()):
                if self._coding.apply_translation(trans) < code:
                    self.properties['canon'] = False
                    return
        self.properties['canon'] = True

    def check_properties(self):
        self.check_subt_extr()
        # self.check_planarity()
        # self.check_sorted()
        self.check_canon()

    # GRAPHICS and FILES

    def draw(self):
        graphics.plot_graph(self.graph, self._coding, self.properties)
        graphics.save_graph_drawing(n=self.n, k=self.k, coding=self._coding)

    def save(self):
        data = {'code': self._coding.to_save(),
                'certificate': bytes.hex(self.certificate) if self.certificate is not None else self.certificate,
                'GAP': self.GAP,
                'properties': self.properties,
                'timings': self.timings}
        utl.save_graph_file(self, data)

    def __repr__(self):
        return f"{str(self._coding):<48}    " + "  ".join(
            f"{k} {utl.bool_symb(v)}" for k, v in self.properties.items())

    # def debug_update_code(self):    # TODO: REMOVE!!!!
    #     try:
    #         orig_pos = nx.planar_layout(self.graph)
    #     except nx.NetworkXException:
    #         orig_pos = nx.shell_layout(self.graph)
    #     code = self.code
    #     perms = tuple(self.original_code.code_permutations())
    #     rows = len(perms)
    #     for j, comp in enumerate(perms):
    #         # print(comp)
    #         transls = tuple(comp[0].get_translations())
    #         cols = len(transls)
    #         for i, trans in enumerate(transls):
    #             new_code = self.original_code.apply_translation(trans)
    #             self.code, self.graph = new_code, _get_graph_from_code(new_code)
    #             code = min(code, new_code)
    #             # print(f"\t{new_code} - {code}")
    #             plt.subplot(rows, cols, j * cols + i + 1)
    #             pos = dict((trans[k], i) for k, i in orig_pos.items())
    #             self.draw(pos)
    #     fig = plt.gcf()
    #     fig.set_size_inches(cols * 4, rows * 4)
    #     plt.show()
    #     # fig.savefig(os.path.join(var.DATA_DIR, "temp.png"))
    #     self.code = code


if __name__ == '__main__':
    # G = ClovenGraph(7, (((0, 1, 2, 3), (4, 5, 6)), ((1, 5, 6), (0, 2, 3, 4))))  # SEP Y
    # G = ClovenGraph(6, (((0, 1, 2, 3), (4, 5)), ((0, 2), (1, 3), (4, 5))))  # NO SEP
    G = ClovenGraph(6, (((0, 1, 2, 3), (4, 5)), ((0, 3, 4), (1, 5, 2))))  # SEP Y, extr Y
    # G = ClovenGraph(7, (((0, 2, 4, 5), (1, 6, 3)), ((1, 2, 3, 5), (0, 4, 6))))  # SEP Y, extr N
    G.check_properties()
    print(G.properties)
    print(G.check_subt_extr())
    G.calc_GAP()
    print(f"GAP is {G.GAP}")
    G.draw()
    plt.savefig("../temp.png")
