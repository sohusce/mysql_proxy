#include <Python.h>
#include "sql-tokenizer.h"

static PyObject *tokenize(PyObject *self, PyObject *args) {
	char *sql = NULL;
	if (!PyArg_ParseTuple(args, "s", &sql))
		return NULL;
	GPtrArray *tokens = sql_tokens_new();
	int result = sql_tokenizer(tokens, sql, strlen(sql));
	PyObject *list = PyList_New(tokens->len);
	//Py_INCREF(list);
	gsize i;
	for (i=0; i<tokens->len; i++) {
		sql_token *token = tokens->pdata[i];
		GString *upper = g_string_ascii_up(token->text);
		PyList_SetItem(list, i, Py_BuildValue("(s,i)", upper->str, token->token_id));
	}
	sql_tokens_free(tokens);
	return list;
}

static PyMethodDef sce_tokenizeMethods[] = {
	{"tokenize", tokenize, METH_VARARGS, "SQL tokenizer"},
	{NULL, NULL, 0, NULL}
};

PyMODINIT_FUNC initsce_tokenize() {
	Py_InitModule("sce_tokenize", sce_tokenizeMethods);
}
