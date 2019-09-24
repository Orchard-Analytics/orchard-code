
import sys
import json

if '--models' not in sys.argv:
    raise ValueError('Missing `--models` parameter')


PROJECT_NAME = ""
RAW_SCHEMA_NAME = ""
RAW_SCHEMA_VARIABLE_NAME = ""
TARGET_SCHEMA = ""

ROOT = sys.path[0]
MANIFEST_FILE_NAME = 'manifest.json'
MANIFEST_FILE_PATH = ROOT + "/compiled/" + MANIFEST_FILE_NAME
MODEL_ROOT = '{}.{}'.format(TARGET_SCHEMA, PROJECT_NAME)


class colors:
    PASS = '\033[92m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'


with open(MANIFEST_FILE_PATH) as fp:
    manifest = json.load(fp)

macros = manifest['macros']
models = manifest['nodes']
model_param_index = sys.argv.index('--models')
model_list = sys.argv[model_param_index + 1:]


def run():
    for model_name in model_list:
        try:
            model_lookup = MODEL_ROOT + '.%s' % model_name
            model = models[model_lookup]
            dbt_sql = model['raw_sql']
            to_sql(model_name, dbt_sql)
        except KeyError:
            if model_lookup not in models.keys():
                _printer(model_name, is_error=True)


def to_sql(model_name, dbt_sql):
    query_start = False
    l = []
    for line in dbt_sql.splitlines():
        if 'select' not in line and 'with' not in line:
            pass
        else:
            query_start = True
        if query_start:
            l.append(_line_to_sql(line) + '\n')
    _printer(model_name, is_error=False, sql=l)


def _line_to_sql(line):
    if '{{' in line and 'ref' in line:
        return _convert_ref(line)
    elif '{{' in line and 'var' in line:
        return _convert_var(line)
    else:
        return line


def _convert_var(line):
    line = line.replace( ( "{{ var('%s') }}" % RAW_SCHEMA_VARIABLE_NAME ) , RAW_SCHEMA_NAME )
    return line


def _convert_ref(line):
    line = line.replace("{{ ref('", ("%s." % TARGET_SCHEMA ) ).replace("') }}", '')
    return line

def _printer(model_name, is_error, sql=None):
    if is_error:
        color = colors.FAIL
        print('%s\n*****************\n' % color)
        print("'%s' does not identify any models" % model_name)
        print('\n*****************\n' + colors.ENDC)
    else:
        color = colors.PASS
        print('%s\n*****************\n' % color)
        print(model_name)
        print('\n*****************\n' + colors.ENDC)
        print(''.join(sql))


run()
