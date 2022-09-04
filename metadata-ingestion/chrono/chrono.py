import os
import re
import parsec as p
import time
from dataclasses import dataclass
from typing import List
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.schema_registry import SchemaRegistryClient, Schema, SchemaReference
from ruamel.yaml import YAML

yaml = YAML(typ='safe')


ATT = '/Users/tao.sun/tao/all-the-things'
IMPORT = 'import'
PACKAGE = 'package'


@dataclass
class Model:
    name: str
    package: str
    subject: str
    references: List[str]
    protobuf: str
    message: str 

@dataclass
class Idol:
    name: str
    model: Model


class IdolKafka:
    def __init__(self, server_url):
        self.client = admin_client = AdminClient({
             'bootstrap.servers': server_url
        })

    def create_topic(self, idol):
        topic = f'{idol.name}.{idol.model.name}'
        topics = [NewTopic(f'{topic}', 1, 1)]
        self.client.create_topics(topics)
        time.sleep(3)
        return topic


class IdolRegistry:
    def __init__(self, url: str):
        self.registered = set()
        self.client = SchemaRegistryClient({
            'url': url
        })

    def register_model(self, model: Model):
        if model.subject not in self.registered:
            references = [SchemaReference(name=x,subject=x,version=1) for x in model.references]
            schema = Schema(
                model.protobuf,
                'PROTOBUF',
                references
            )
            self.client.register_schema(model.subject, schema)
            self.registered.add(model.subject)


proto_import = (
    p.string(IMPORT) >>
    p.spaces() >>
    p.string('"') >>
    p.regex(r'[^"]+') <<
    p.string('"') <<
    p.spaces() <<
    p.string(';')
).parsecmap(lambda imp: {IMPORT: imp})

ident_pat = '[a-zA-Z]([a-zA-Z0-9_]*)'
full_ident_pat = ident_pat + r'(\.' + ident_pat + ')*'

proto_package = (
    p.string('package') >>
    p.spaces() >>
    p.regex(full_ident_pat) <<
    p.spaces() <<
    p.string(';')
).parsecmap(lambda package: {PACKAGE: package})


class ProtoBufParser:
    def __init__(self, content: str):
        self.content = content

    def get_imports(self):
        imports = []
        content = re.sub('//.*', '', self.content)
        indices = [m.start() for m in re.finditer(IMPORT, content)]
        for indice in indices:
            result = proto_import.parse(content[indice:])
            if result is not None:
                if isinstance(result, dict):
                    if IMPORT in result:
                        imports.append(result[IMPORT])
        imports = [x for x in imports if not x.startswith('google')]
        return imports

    def get_package(self) -> str:
        package = ''
        indices = [m.start() for m in re.finditer(PACKAGE, self.content)]
        for indice in indices:
            package = proto_package.parse(self.content[indice:])
            break
        return package[PACKAGE]


def get_content(relative_path: str) -> str:
    p = os.path.join(ATT, relative_path)
    with open(p, 'r') as f:
        content = f.read()
    return content


def find_models(model_names: List[str]):
    def get_references(parser: ProtoBufParser, collected: set) -> List[str]:
        imports = set([x for x in parser.get_imports() if x not in collected])
        collected = collected.union(imports)
        for reference in imports:
            indirect_content = get_content(reference)
            indirect_parser = ProtoBufParser(content=indirect_content)
            indirect_references = get_references(indirect_parser, collected)
            collected = collected.union(indirect_references)
        return collected
    models = []
    for d, _, ns in os.walk(ATT):
        for n in ns:
            if n.endswith('.proto'):
                p = os.path.join(d, n)
                r = p.replace(f'{ATT}/', '')
                content = get_content(r)
                parser = ProtoBufParser(content=content)
                for model_name in model_names:
                    pattern = f'message {model_name}'
                    if pattern in content:
                        # make model
                        package = parser.get_package()
                        model = Model(
                            name=model_name,
                            package=package,
                            subject=r,
                            references=get_references(parser, set()),
                            protobuf=content,
                            message=f'{package}.{model_name}'
                        )
                        models.append(model)
    return models


def get_chrono_queries():
    config_path = 'chrono/spark/etc/configs/config.yaml'
    config_full_path = os.path.join(ATT, config_path)
    with open(config_full_path, 'r') as f:
        config = yaml.load(f)

    queries = config.get('jobs', {}).get('projected', {}).get('queries')
    return queries


def get_chrono_idol_iterator():
    queries = get_chrono_queries().items()
    idol_model_names = [value.get('idol_model_name') for (_, value) in queries]
    models = find_models(idol_model_names)

    for (query_name, query_value) in queries:
        idol_name = query_value.get('idol_name')
        idol_model_name = query_value.get('idol_model_name')
        model = next(x for x in models if x.name == idol_model_name)
        if model:
            yield Idol(
                name=idol_name.replace('.', '-'),
                model=model
            ) 


def generate_recipe(message, topic, subject):
    with open('./kafka.template') as f:
        template = yaml.load(f)
    source = template.get('source', {})
    config = source.get('config', {})
    topic_patterns = config.get('topic_patterns', {})

    topic_patterns.update({'allow': [f'^{topic}$']})
    ts_map = {f'{topic}-value': subject}
    config.update({'topic_subject_map': ts_map})
    tm_map = {topic: message}
    config.update({'topic_message_map': tm_map})
    with open(f'./recipe/{topic}.yaml', 'w') as f:
        yaml.dump(template, f)

def main():
    registry = IdolRegistry(url='http://localhost:8081')
    idol_kafka = IdolKafka(server_url='localhost:9092')
    for idol in get_chrono_idol_iterator():
        topic = idol_kafka.create_topic(idol)
        print(f'Processing >>> {topic}')
        registry.register_model(idol.model)
        generate_recipe(idol.model.message, topic, idol.model.subject)
        

if __name__ == '__main__':
    main()
