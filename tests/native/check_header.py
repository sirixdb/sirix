#!/usr/bin/env python3
"""NativeHeaderContract: execute consumers of the ABI 1.0 declarative contract.

The independently reviewed manifest comes from NATIVE §3, not header extraction.
No libsirix implementation is linked; generated definitions only test C linkage.
"""
import argparse
import json
import os
from pathlib import Path
import shlex
import subprocess
import sys
import tempfile

import cffi

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent.parent


def run(args, **kwargs):
    return subprocess.run(args, check=True, text=True, capture_output=True, **kwargs)


def declaration(ctype, name):
    if '[' in ctype:
        base, extent = ctype.split('[', 1)
        return f'{base} {name}[{extent}'
    return f'{ctype} {name}'


def native_source(manifest, cpp):
    lines = ['#include "sirix.h"', '#include "sirix.h"', '#include <stddef.h>',
             '#include <stdint.h>', '#include <limits.h>']
    if cpp:
        lines += ['#include <type_traits>', '#define CHECK static_assert',
                  '#define ALIGNOF alignof']
    else:
        lines += ['#define CHECK _Static_assert', '#define ALIGNOF _Alignof']
    lines += ['CHECK(CHAR_BIT == 8, "byte width");',
              'CHECK(sizeof(void *) == 8, "pointer width");',
              'CHECK(sizeof(sirix_status_t) == 4, "status width");',
              'CHECK((sirix_status_t)-1 < 0, "signed status");',
              'CHECK(sizeof(sirix_handle_t) == 8, "handle width");',
              'CHECK((sirix_handle_t)-1 > 0, "unsigned handle");']
    for name, value in manifest['constants'].items():
        lines.append(f'CHECK({name} == {value}, "{name}");')
    for typ, layout in manifest['structs'].items():
        lines += [f'CHECK(sizeof({typ}) == {layout["size"]}, "size {typ}");',
                  f'CHECK(ALIGNOF({typ}) == {layout["alignment"]}, "align {typ}");']
        for field, contract in layout['fields'].items():
            alias = f'expected_{typ}_{field}'
            lines.append(f'typedef {declaration(contract["type"], alias)};')
            lines.append(f'CHECK(offsetof({typ}, {field}) == {contract["offset"]}, "offset {typ}.{field}");')
            if cpp:
                expr = f'(std::is_same<decltype((({typ} *)0)->{field}), {alias}>::value)'
            else:
                expr = f'_Generic(&(({typ} *)0)->{field}, {alias} *: 1, default: 0)'
            lines.append(f'CHECK({expr}, "type {typ}.{field}");')
    for name, (result, args) in manifest['functions'].items():
        lines.append(f'typedef {result} (SIRIX_CALL *expected_{name})({", ".join(args) or "void"});')
        expr = (f'(std::is_same<decltype(&{name}), expected_{name}>::value)' if cpp
                else f'_Generic(&{name}, expected_{name}: 1, default: 0)')
        lines.append(f'CHECK({expr}, "signature {name}");')
    lines.append('int main(void) {')
    lines += ['  const uint16_t endian = 1;',
              '  if (*(const uint8_t *)&endian != 1) return 1;']
    # Check every field of each public initializer, including nested headers.
    def check_fields(typ, expr, expected):
        layout = manifest['structs'][typ]
        for field, contract in layout['fields'].items():
            value_expr = f'{expr}.{field}'
            field_type = contract['type']
            if field_type in manifest['structs']:
                check_fields(field_type, value_expr, {})
            elif '[' in field_type:
                lines.append(f'  for (size_t i = 0; i < sizeof({value_expr}); ++i) if ({value_expr}[i] != 0) return 2;')
            else:
                value = (layout['size'] if field == 'struct_size' else
                         1 if field == 'abi_major' else expected.get(field, 0))
                lines.append(f'  if ({value_expr} != {value}) return 3;')
    for index, (typ, (macro, expected)) in enumerate(manifest['defaults'].items()):
        lines.append(f'  const {typ} value{index} = {macro};')
        check_fields(typ, f'value{index}', expected)
    if cpp:
        for name in manifest['functions']:
            lines += [f'  expected_{name} volatile ref_{name} = &{name};',
                      f'  if (ref_{name} == nullptr) return 4;']
    lines += ['  return 0;', '}']
    return '\n'.join(lines) + '\n'


def linkage_stubs(manifest):
    lines = ['#include "sirix.h"']
    for name, (result, args) in manifest['functions'].items():
        params = ', '.join(f'{typ} a{i}' for i, typ in enumerate(args)) or 'void'
        lines.append(f'{result} SIRIX_CALL {name}({params}) {{')
        lines.extend(f'  (void)a{i};' for i in range(len(args)))
        lines += ['  return 0;', '}']
    return '\n'.join(lines) + '\n'


def check_native(header, out, manifest, cc, cxx):
    c_file, cpp_file, stub = out/'contract.c', out/'contract.cpp', out/'linkage.c'
    c_file.write_text(native_source(manifest, False))
    cpp_file.write_text(native_source(manifest, True))
    stub.write_text(linkage_stubs(manifest))
    flags = ['-Wall', '-Wextra', '-Werror', '-pedantic', '-I', str(header.parent)]
    run(cc + ['-std=c11', *flags, str(c_file), '-o', str(out/'c-contract')])
    run([str(out/'c-contract')])
    run(cc + ['-std=c11', *flags, '-c', str(stub), '-o', str(out/'linkage.o')])
    run(cxx + ['-std=c++17', *flags, str(cpp_file), str(out/'linkage.o'), '-o', str(out/'cpp-contract')])
    run([str(out/'cpp-contract')])


def check_cffi(header, out, manifest, cc):
    # Use the real C preprocessor; SIRIX_FFI suppresses decoration only.
    source = run(cc + ['-std=c11', '-E', '-P', '-DSIRIX_FFI', '-x', 'c', str(header)]).stdout
    (out/'sirix.cdef').write_text(source)
    ffi = cffi.FFI()
    ffi.cdef(source)
    for typ, layout in manifest['structs'].items():
        assert ffi.sizeof(typ) == layout['size'], typ
        assert ffi.alignof(typ) == layout['alignment'], typ
        actual = dict(ffi.typeof(typ).fields)
        assert set(actual) == set(layout['fields']), typ
        for field, contract in layout['fields'].items():
            assert ffi.offsetof(typ, field) == contract['offset'], (typ, field)
            assert actual[field].type == ffi.typeof(contract['type']), (typ, field)
    # cffi's parsed declaration model, not a source-text pattern, owns this set.
    functions = {key.removeprefix('function ') for key in ffi._parser._declarations
                 if key.startswith('function sirix_')}
    assert functions == set(manifest['functions']), functions
    for name, (result, args) in manifest['functions'].items():
        signature = f'{result}(*)({", ".join(args) or "void"})'
        parsed = ffi._parser._declarations['function ' + name][0]
        assert parsed == ffi._parser.parse_type(signature), name


def check_rust(header, out, manifest, bindgen, rustc):
    generated = out/'bindings.rs'
    run(bindgen + [str(header), '--allowlist-type', 'sirix_.*',
                   '--allowlist-function', 'sirix_.*', '--allowlist-var', 'SIRIX_.*',
                   '--output', str(generated), '--', '-std=c11'])
    assertions = ['#[test] fn native_abi_manifest() {']
    for typ, layout in manifest['structs'].items():
        assertions += [f'assert_eq!(std::mem::size_of::<{typ}>(), {layout["size"]});',
                       f'assert_eq!(std::mem::align_of::<{typ}>(), {layout["alignment"]});']
        for field, contract in layout['fields'].items():
            # bindgen escapes Rust reserved keywords with a trailing underscore.
            rust_field = 'type_' if field == 'type' else field
            assertions.append(f'assert_eq!(std::mem::offset_of!({typ}, {rust_field}), {contract["offset"]});')
    for name, value in manifest['constants'].items():
        assertions.append(f'assert_eq!({name} as u64, {value});')
    assertions.append('}')
    with generated.open('a') as output:
        output.write('\n'+'\n'.join(assertions)+'\n')
    run(rustc + ['--edition=2021', '--test', str(generated), '-o', str(out/'rust-contract')])
    run([str(out/'rust-contract')])


def check_mutations(header, out, manifest, cc, cxx):
    # Scripted mutations test whether real consumers reject broken contracts.
    mutations = [
        ('status-number', '#define SIRIX_BUSY 109', '#define SIRIX_BUSY 209'),
        ('version', '#define SIRIX_ABI_MAJOR 1', '#define SIRIX_ABI_MAJOR 2'),
        ('width', 'typedef int32_t sirix_status_t;', 'typedef int64_t sirix_status_t;'),
        ('signedness', 'typedef uint64_t sirix_handle_t;', 'typedef int64_t sirix_handle_t;'),
        ('layout', 'int32_t code, detail_code;', 'int32_t detail_code, code;'),
        ('default', '#define SIRIX_DEFAULT_WORKERS 2', '#define SIRIX_DEFAULT_WORKERS 3'),
        ('capability', '#define SIRIX_CAP_REQUIRED_1_0 255', '#define SIRIX_CAP_REQUIRED_1_0 253'),
        ('linkage', 'extern "C" {', 'extern "C++" {'),
    ]
    original = header.read_text()
    for label, before, after in mutations:
        if original.count(before) != 1:
            raise RuntimeError(f'Mutation fixture needs maintenance: {label}')
        mutant = out/label
        mutant.mkdir()
        altered = mutant/'sirix.h'
        altered.write_text(original.replace(before, after, 1))
        try:
            check_native(altered, mutant, manifest, cc, cxx)
        except subprocess.CalledProcessError as failure:
            (mutant/'rejection.txt').write_text(failure.stdout + failure.stderr)
        else:
            raise AssertionError(f'Surviving mutant: {label}')
    print(f'NativeHeaderContract: {len(mutations)} / {len(mutations)} contract mutants killed')


def main():
    if not __debug__:
        raise RuntimeError('Run without -O: contract assertions must be enabled')
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output-dir', type=Path, required=True)
    parser.add_argument('--header', type=Path, default=ROOT/'include/sirix.h')
    parser.add_argument('--mutations', action='store_true')
    args = parser.parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)
    manifest = json.loads((HERE/'abi-layout.json').read_text())
    symbols = (HERE/'symbols.txt').read_text().splitlines()
    assert symbols == sorted(manifest['functions']), 'symbol manifest'
    cc = shlex.split(os.environ.get('CC', 'cc'))
    cxx = shlex.split(os.environ.get('CXX', 'c++'))
    out = Path(tempfile.mkdtemp(prefix='header-', dir=args.output_dir))
    print(f'NativeHeaderContract: evidence directory {out}', flush=True)
    try:
        check_native(args.header, out, manifest, cc, cxx)
        check_cffi(args.header, out, manifest, cc)
        check_rust(args.header, out, manifest,
                   shlex.split(os.environ.get('BINDGEN', 'bindgen')),
                   shlex.split(os.environ.get('RUSTC', 'rustc')))
        if args.mutations:
            check_mutations(args.header, out, manifest, cc, cxx)
    except subprocess.CalledProcessError as failure:
        (out/'failure.txt').write_text(failure.stdout + failure.stderr)
        print(failure.stdout + failure.stderr, file=sys.stderr)
        raise
    print('NativeHeaderContract: C11, C++17/C linkage, cffi and bindgen/Rust passed')


if __name__ == '__main__':
    main()
