{application, loader,
 [{description, " loader"},
  {vsn, "1.0.0"},
  {modules, [loader_app,
             loader_sup,
	     loader_lib,
	     loader]},
  {registered, [loader]},
  {applications, [kernel, stdlib]},
  {mod, {loader_app, []}}
 ]}.
