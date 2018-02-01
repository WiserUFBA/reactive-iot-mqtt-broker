/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package br.ufba.dcc.wiser.util;

import java.util.concurrent.Callable;
/**
 *
 * @author elisama
 */
public class Calable {
 

  public static <T> T executeWithTCCLSwitch(Callable<T> action) throws Exception {
    final ClassLoader original = Thread.currentThread().getContextClassLoader();
    try {
      Thread.currentThread().setContextClassLoader(Calable.class.getClassLoader());
      return action.call();
    } finally {
      Thread.currentThread().setContextClassLoader(original);
    }
  }

  public static void executeWithTCCLSwitch(Runnable action) {
    final ClassLoader original = Thread.currentThread().getContextClassLoader();
    try {
      Thread.currentThread().setContextClassLoader(Calable.class.getClassLoader());
      action.run();
    } finally {
      Thread.currentThread().setContextClassLoader(original);
    }
  }

}   

