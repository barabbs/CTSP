from modules.calculations.gap import GAP_Gurobi
import argparse

# Initialize parser
parser = argparse.ArgumentParser(formatter_class=argparse.RawTextHelpFormatter)

parser.add_argument("-n", type=int,
                    help="number of nodes n in graph for model\n\n", required=True)

def generate_model(n):
    print(f"Generating model file for n={n}...")
    calc = GAP_Gurobi(n=n)
    print(f"Creating model...")
    calc.initialize()
    print(f"Saving...")
    calc.save()
    print(f"Done!")

if __name__ == '__main__':
    args = parser.parse_args()
    generate_model(args.n)