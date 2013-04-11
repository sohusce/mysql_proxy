#! /usr/bin/python 
from distutils.core import setup, Extension 
import setup_posix

options = setup_posix.get_config()
sce_tokenize_mod = Extension("sce_tokenize", **options)
setup(name = "SCE MySQL tokenize extension", 
      version = "1.0", 
      description = "SCE MySQL tokenize extension from MySQL-Proxy", 
      ext_modules = [sce_tokenize_mod])
