GLIB_CFLAGS = -I/usr/include/glib-2.0 -I/usr/lib/x86_64-linux-gnu/glib-2.0/include
GLIB_LIBS = -lglib-2.0
PYTHON_CFLAGS = -I/usr/include/python2.7

sce_tokenize.so:
	gcc src/*.c -shared -fPIC $(GLIB_CFLAGS) $(GLIB_LIBS) $(PYTHON_CFLAGS) -o sce_tokenize.so
