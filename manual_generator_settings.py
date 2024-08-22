from modules import var
from modules.combinatorics import partition, manual_codings_generator
from modules import utility as utl
import json
import argparse

# Initialize parser
parser = argparse.ArgumentParser(formatter_class=argparse.RawTextHelpFormatter)

parser.add_argument("-n", type=int, nargs="+",
                    help="number of nodes n in graph  (sequence separated by whitespace)\n\n", required=True)
parser.add_argument("-k", type=int, nargs="+", default=(2,),
                    help="number of covers k in graph (sequence separated by whitespace)\n\n")
parser.add_argument("-m", "--maximum", type=int, default=None)
parser.add_argument("-s", "--symmetric", action="store_true")
parser.add_argument("-r", "--reverse", action="store_true")
parser.add_argument("-c", "--cut_min", type=int, default=None)
parser.add_argument("-t", "--test", action="store_true")


def create_settings(n, k, maximum=None, symmetric=False, reverse=True, cut_minimum=None):
    maximum = maximum or n - 2
    print(f"Creating new generator settings for k={k}, n={n}\n"
          f"[maximum: {maximum},  symmetric: {symmetric},  reverse: {reverse},  cut_min: {cut_minimum}]")
    if maximum > n - 2:
        print("!!! maximum must be at most n-2 !!!")
        return
    partitions = tuple(partition(n, maximum=maximum, minimum=2))
    if cut_minimum is not None:
        partitions = partitions[:-cut_minimum]
    if reverse:
        partitions = partitions[::-1]
    parts = tuple()
    for i_p, p_1 in enumerate(partitions):
        if symmetric:
            partitions_2 = (p_1,)
        elif reverse:
            partitions_2 = partitions[:i_p + 1]
        else:
            partitions_2 = partitions[i_p:]
        parts += tuple((p_1, p_2) for p_2 in partitions_2)
    print("\n".join(f"\t{utl.seqence_to_str(p)}" for p in parts))
    if input("Sounds good?").upper() != "Y":
        print("Exiting")
        return
    with open(var.GENERATOR_SETTINGS_FILEPATH.format(n=n, k=k), 'w') as f:
        json.dump({'parts': parts}, f)
    print("Saved")

def test_manual_generator(n, k):
    i = 0
    for (c_1, c_2), (p_1, p_2) in manual_codings_generator(n, k):
        print(f"{utl.seqence_to_str((p_1, p_2)):<16}{utl.seqence_to_str(c_1)} - {utl.seqence_to_str(c_2)}")
        i += 1
    print(f"Total {i}")


if __name__ == '__main__':
    args = parser.parse_args()
    for k in args.k:
        for n in args.n:
            if not args.test:
                create_settings(n=n, k=k,
                                maximum=args.maximum, symmetric=args.symmetric,
                                reverse=args.reverse, cut_minimum=args.cut_min)
            else:
                test_manual_generator(n=n, k=k)
