if __name__ == "__main__":
    with open('file.txt', 'r') as f:    
        for line in f:
            fields = line.split(':')
            user_id = fields[0]
            followers = [f for f in fields[1].split(' ') if f.strip()]
            print(f"user id {user_id} follows {[id for id in followers]}")