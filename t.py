with open('benchmark.log', 'r') as f:
    lines = f.read().split('\n')

groups = {}
for line in lines[:]:
    if not line.startswith('vsr.checksum'): continue
    parts = line.split(' = ')

    value = parts[-1]
    if value == '0': continue

    name = '('.join(parts[0].split('(')[1:])
    name = ')'.join(name.split(')')[:-1])
    
    if name not in groups: groups[name] = {}
    groups[name][value] = groups[name][value] + 1 if value in groups[name] else 1

for (name, values) in groups.items():
    print('vsr.checksum(' + name + '):')

    s = {k: v for k, v in sorted(values.items(), key=lambda item: item[1], reverse=True)}
    for (length, count) in s.items():
        print(('(' + str(count) + ')').rjust(9), 'source.len =', length)


    # for (length, count) in values.items():
    #     print(' ', ('(' + str(count) + ')').rjust(7), 'source.len =', length)