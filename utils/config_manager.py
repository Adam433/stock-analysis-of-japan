import json

class ConfigManager:
    def __init__(self, config_path):
        self.config_path = config_path
        self.config = self.load_config()

    def load_config(self):
        """加载配置文件。"""
        with open(self.config_path, 'r') as file:
            return json.load(file)

    def get_config(self, key):
        """获取配置项。"""
        return self.config.get(key)

    def update_config(self, key, value):
        """更新配置项。"""
        self.config[key] = value
        self.save_config()

    def save_config(self):
        """保存更新后的配置回文件。"""
        with open(self.config_path, 'w') as file:
            json.dump(self.config, file, indent=4)
