/**
 * Copyright (c) 2011, University of Konstanz, Distributed Systems Group
 * All rights reserved.
 * 
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 * * Redistributions of source code must retain the above copyright
 * notice, this list of conditions and the following disclaimer.
 * * Redistributions in binary form must reproduce the above copyright
 * notice, this list of conditions and the following disclaimer in the
 * documentation and/or other materials provided with the distribution.
 * * Neither the name of the University of Konstanz nor the
 * names of its contributors may be used to endorse or promote products
 * derived from this software without specific prior written permission.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> BE LIABLE FOR ANY
 * DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package org.sirix.gui.view.controls;

import java.awt.event.KeyEvent;
import java.awt.event.MouseEvent;
import java.awt.event.MouseWheelEvent;

import org.sirix.gui.view.VisualItem;

/**
 * Abstract class to simplify {@link Control} implementation. Only necessary
 * methods have to be overriden.
 * 
 * @author Johannes Lichtenberger, University of Konstanz
 * 
 */
public abstract class AbstractControl implements Control {

	/** {@inheritDoc} */
	@Override
	public boolean isEnabled() {
		return false;
	}

	/** {@inheritDoc} */
	@Override
	public void setEnabled(boolean enabled) {
	}

	/** {@inheritDoc} */
	@Override
	public void itemDragged(VisualItem paramItem, MouseEvent paramEvent) {
	}

	/** {@inheritDoc} */
	@Override
	public void itemMoved(VisualItem paramItem, MouseEvent paramEvent) {
	}

	/** {@inheritDoc} */
	@Override
	public void itemWheelMoved(VisualItem paramItem, MouseWheelEvent paramEvent) {
	}

	/** {@inheritDoc} */
	@Override
	public void itemClicked(VisualItem paramItem, MouseEvent paramEvent) {
	}

	/** {@inheritDoc} */
	@Override
	public void itemPressed(VisualItem paramItem, MouseEvent paramEvent) {
	}

	/** {@inheritDoc} */
	@Override
	public void itemReleased(VisualItem paramItem, MouseEvent paramEvent) {
	}

	/** {@inheritDoc} */
	@Override
	public void itemEntered(VisualItem paramItem, MouseEvent paramEvent) {
	}

	/** {@inheritDoc} */
	@Override
	public void itemExited(VisualItem paramItem, MouseEvent paramEvent) {
	}

	/** {@inheritDoc} */
	@Override
	public void itemKeyPressed(VisualItem paramItem, KeyEvent paramEvent) {
	}

	/** {@inheritDoc} */
	@Override
	public void itemKeyReleased(VisualItem paramItem, KeyEvent paramEvent) {
	}

	/** {@inheritDoc} */
	@Override
	public void itemKeyTyped(VisualItem paramItem, KeyEvent paramEvent) {
	}

	/** {@inheritDoc} */
	@Override
	public void mouseEntered(MouseEvent paramEvent) {
	}

	/** {@inheritDoc} */
	@Override
	public void mouseExited(MouseEvent paramEvent) {
	}

	/** {@inheritDoc} */
	@Override
	public void mousePressed(MouseEvent paramEvent) {
	}

	/** {@inheritDoc} */
	@Override
	public void mouseReleased(MouseEvent paramEvent) {
	}

	/** {@inheritDoc} */
	@Override
	public void mouseClicked(MouseEvent paramEvent) {
	}

	/** {@inheritDoc} */
	@Override
	public void mouseDragged(MouseEvent paramEvent) {
	}

	/** {@inheritDoc} */
	@Override
	public void mouseMoved(MouseEvent paramEvent) {
	}

	/** {@inheritDoc} */
	@Override
	public void mouseWheelMoved(MouseWheelEvent paramEvent) {
	}

	/** {@inheritDoc} */
	@Override
	public void keyPressed(KeyEvent paramEvent) {
	}

	/** {@inheritDoc} */
	@Override
	public void keyReleased(KeyEvent paramEvent) {
	}

	/** {@inheritDoc} */
	@Override
	public void keyPressed() {
	}

	/** {@inheritDoc} */
	@Override
	public void keyReleased() {
	}

	/** {@inheritDoc} */
	@Override
	public void keyTyped(KeyEvent paramEvent) {
	}
}
