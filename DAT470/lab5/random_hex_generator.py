import random

def generate_hex_numbers(count, filename):
    with open(filename, 'w') as f:
        for _ in range(count):
            num = random.randint(0, 0xFFFFFFFF)
            f.write(f'0x{num:08x}\n')

if __name__ == "__main__":
    generate_hex_numbers(1000, 'hex_numbers.txt')
