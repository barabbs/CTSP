import os, enlighten
from modules import var
from sqlalchemy import create_engine, select, insert, update, bindparam, func
from sqlalchemy.orm import Session
from modules.models import get_models, GRAPH, TIMINGS


def update_database_old(n, k=2, weights=None, donor=None, recip=None):
    print(f"Updating {recip} with {donor}")
    manager = enlighten.get_manager()
    weights = weights or (1,) * k
    metadata, models = get_models(n, k, weights)
    g_mod = models[GRAPH]
    don_eng = create_engine(f"sqlite:///{os.path.join(var.DATABASE_DIR, donor)}")
    rec_eng = create_engine(f"sqlite:///{os.path.join(var.DATABASE_DIR, recip)}")
    with (Session(don_eng) as don_sess):
        with Session(rec_eng) as rec_sess:
            rec_graphs = rec_sess.execute(select(g_mod.coding, g_mod.certificate)).all()
            total = len(rec_graphs)
            progbar = manager.counter(total=total, desc=f"Updating...", leave=False)
            updated = 0
            for coding, cert in rec_graphs:
                print(coding, cert)
                don_graph = don_sess.execute(
                    select(
                        g_mod.prop_subt, g_mod.prop_extr
                    ).where(
                        g_mod.certificate.is_(cert), g_mod.prop_subt.is_not(None)
                    )
                ).one_or_none()
                print(don_graph)
                if don_graph is None:
                    progbar.update()
                    continue
                rec_sess.execute(update(g_mod).where(g_mod.coding == bindparam("id")),
                                 ({
                                      "id": coding,
                                      "prop_subt": don_graph[0],
                                      "prop_extr": don_graph[1],
                                  },)
                                 )
                progbar.update()
                updated += 1
            rec_sess.commit()
    progbar.close()
    print(f"Updated {updated} out of {total}")
    manager.stop()

STEP = 1000
def update_database(n, k=2, weights=None, donor=None, recip=None):
    print(f"Updating {recip} with {donor}")
    manager = enlighten.get_manager()
    weights = weights or (1,) * k
    metadata, models = get_models(n, k, weights)
    g_mod = models[GRAPH]
    don_eng = create_engine(f"sqlite:///{os.path.join(var.DATABASE_DIR, donor)}", echo=True)
    rec_eng = create_engine(f"sqlite:///{os.path.join(var.DATABASE_DIR, recip)}", echo=True)
    with (Session(don_eng) as don_sess):
        with Session(rec_eng) as rec_sess:
            don_graphs = don_sess.execute(select(
                g_mod.certificate, g_mod.prop_subt, g_mod.prop_extr, g_mod.gap
            ).where(
                g_mod.prop_subt.is_not(None)
            )).all()
            total = len(don_graphs)
            print(f"found {len(don_graphs)}")
            to_update = tuple({
                                  "cert": cert,
                                  "prop_subt": prop_subt,
                                  "prop_extr": prop_extr,
                                  "gap": gap
                              } for cert, prop_subt, prop_extr, gap in don_graphs)
            progbar = manager.counter(total=total, desc=f"Updating...", leave=False)
            for i in range(0, total, STEP):
                chunk = to_update[i:i + STEP]
                rec_sess.execute(update(g_mod).where(g_mod.certificate == bindparam("cert")),
                                 chunk
                                 )
                rec_sess.commit()
                progbar.update(incr=STEP)
    progbar.close()
    print(f"Updated!")
    manager.stop()


if __name__ == '__main__':
    update_database(n=11, donor="k2_n11_w1-1_h00001.cgdb", recip="k2_n11_w1-1_m00001.cgdb")
